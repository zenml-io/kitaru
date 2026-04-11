"""Tests for the kitaru CLI."""

from __future__ import annotations

import importlib
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest
from zenml.exceptions import EntityExistsError

from kitaru.analytics import AnalyticsEvent
from kitaru.cli import (
    RuntimeSnapshot,
    _build_runtime_snapshot,
    _describe_local_server,
    _logout_current_connection,
    _parse_secret_assignments,
    app,
)
from kitaru.client import ExecutionStatus, LogEntry
from kitaru.config import (
    KITARU_MODEL_REGISTRY_ENV,
    ActiveEnvironmentVariable,
    AzureMLStackSpec,
    KubernetesStackSpec,
    ModelAliasConfig,
    ModelRegistryConfig,
    SagemakerStackSpec,
    StackComponentConfigOverrides,
    StackType,
    VertexStackSpec,
)


class _BrokenGlobalConfig:
    """Test double that simulates a missing local ZenML store backend."""

    config_directory = "/tmp/test-zenml-config"
    local_stores_path = "/tmp/test-zenml-config/local_stores"

    @property
    def store_configuration(self) -> object:
        raise ImportError("sqlalchemy missing")

    @property
    def uses_local_store(self) -> bool:
        raise AssertionError("uses_local_store should not be reached")


def _execution_stub(
    *,
    exec_id: str,
    flow_name: str,
    status: ExecutionStatus,
    stack_name: str | None = "prod",
    pending_wait: SimpleNamespace | None = None,
    failure: SimpleNamespace | None = None,
    status_reason: str | None = None,
    checkpoints: list[SimpleNamespace] | None = None,
) -> SimpleNamespace:
    """Build a lightweight execution-shaped object for CLI tests."""
    return SimpleNamespace(
        exec_id=exec_id,
        flow_name=flow_name,
        status=status,
        started_at=datetime(2026, 3, 7, 10, 0, 0),
        ended_at=datetime(2026, 3, 7, 10, 1, 0),
        stack_name=stack_name,
        pending_wait=pending_wait,
        failure=failure,
        status_reason=status_reason,
        metadata={},
        artifacts=[],
        frozen_execution_spec=None,
        original_exec_id=None,
        checkpoints=checkpoints or [],
    )


def _stack_create_result_stub(
    *,
    name: str = "dev",
    is_active: bool = True,
    previous_active_stack: str | None = "default",
    stack_type: str = "local",
    components_created: tuple[str, ...] | None = None,
    service_connectors_created: tuple[str, ...] = (),
    resources: dict[str, str] | None = None,
) -> SimpleNamespace:
    """Build a lightweight stack-create result object for CLI tests."""
    return SimpleNamespace(
        stack=SimpleNamespace(id=f"stack-{name}-id", name=name, is_active=is_active),
        previous_active_stack=previous_active_stack,
        components_created=components_created
        or (f"{name} (orchestrator)", f"{name} (artifact_store)"),
        stack_type=stack_type,
        service_connectors_created=service_connectors_created,
        resources=resources,
    )


def _stack_details_stub(
    *,
    name: str = "my-k8s",
    stack_id: str | None = None,
    is_active: bool = True,
    is_managed: bool = True,
    stack_type: str = "kubernetes",
    components: list[SimpleNamespace] | None = None,
) -> SimpleNamespace:
    """Build a lightweight stack-details object for `stack show` CLI tests."""
    return SimpleNamespace(
        stack=SimpleNamespace(
            id=stack_id or f"stack-{name}-id",
            name=name,
            is_active=is_active,
        ),
        is_managed=is_managed,
        stack_type=stack_type,
        components=components
        if components is not None
        else [
            SimpleNamespace(
                role="runner",
                name=f"{name}-runner",
                backend="kubernetes",
                details=(
                    ("cluster", "demo-cluster"),
                    ("region", "us-east-1"),
                    ("namespace", "default"),
                ),
                purpose=None,
            ),
            SimpleNamespace(
                role="storage",
                name=f"{name}-storage",
                backend="s3",
                details=(("location", "s3://bucket/kitaru"),),
                purpose=None,
            ),
            SimpleNamespace(
                role="image_registry",
                name=f"{name}-registry",
                backend="aws",
                details=(("location", "123456789012.dkr.ecr.us-east-1.amazonaws.com"),),
                purpose=None,
            ),
        ],
    )


def _write_stack_create_file(tmp_path: Path, content: str) -> Path:
    """Write a temporary stack-create YAML file for CLI tests."""
    path = tmp_path / "stack.yaml"
    path.write_text(content)
    return path


def test_importing_cli_does_not_resolve_version_metadata() -> None:
    """Importing `kitaru.cli` should not resolve package metadata."""
    import kitaru.cli as cli_module

    with patch(
        "kitaru._version.resolve_installed_version",
        side_effect=AssertionError("should not resolve version at import time"),
    ):
        reloaded = importlib.reload(cli_module)
        assert reloaded.app.version == "unknown"

    importlib.reload(cli_module)


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """--version prints the lazily resolved package version and exits."""
    import kitaru.cli as cli_module

    reloaded = importlib.reload(cli_module)
    with patch("kitaru.cli.resolve_installed_version", return_value="9.9.9"):
        reloaded._apply_runtime_version()
        with pytest.raises(SystemExit) as exc_info:
            reloaded.app(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "9.9.9" in captured.out


def test_short_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """-V also prints the lazily resolved package version."""
    import kitaru.cli as cli_module

    reloaded = importlib.reload(cli_module)
    with patch("kitaru.cli.resolve_installed_version", return_value="8.8.8"):
        reloaded._apply_runtime_version()
        with pytest.raises(SystemExit) as exc_info:
            reloaded.app(["-V"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "8.8.8" in captured.out


def test_help_flag_lists_available_commands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--help prints help text, including the currently supported commands."""
    with pytest.raises(SystemExit) as exc_info:
        app(["--help"])
    assert exc_info.value.code == 0
    output = capsys.readouterr().out.lower()
    assert "kitaru" in output
    for command in (
        "init",
        "login",
        "logout",
        "status",
        "info",
        "log-store",
        "stack",
        "secrets",
        "model",
        "executions",
    ):
        assert command in output


def test_no_args_shows_help(capsys: pytest.CaptureFixture[str]) -> None:
    """Invoking with no arguments shows help output."""
    with pytest.raises(SystemExit) as exc_info:
        app([])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "kitaru" in captured.out.lower()


class TestInit:
    """Tests for ``kitaru init``."""

    def test_creates_kitaru_directory(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """``kitaru init <path>`` creates the repository marker directory."""
        target = tmp_path / "myproject"
        target.mkdir()
        with pytest.raises(SystemExit) as exc_info:
            app(["init", str(target)])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "initialized" in captured.out.lower()
        # ZenML creates a repo directory under the target.
        # The exact name depends on whether ZENML_REPOSITORY_DIRECTORY_NAME
        # is supported by the installed ZenML version (.kitaru or .zen).
        assert (target / ".kitaru").is_dir() or (target / ".zen").is_dir()

    def test_fails_on_existing_kitaru_marker(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Should fail when .kitaru/ already exists."""
        target = tmp_path / "existing"
        target.mkdir()
        (target / ".kitaru").mkdir()
        with pytest.raises(SystemExit) as exc_info:
            app(["init", str(target)])
        assert exc_info.value.code == 1
        assert "already initialized" in capsys.readouterr().err.lower()

    def test_fails_on_existing_zen_marker(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Should fail when legacy .zen/ already exists."""
        target = tmp_path / "legacy"
        target.mkdir()
        (target / ".zen").mkdir()
        with pytest.raises(SystemExit) as exc_info:
            app(["init", str(target)])
        assert exc_info.value.code == 1
        assert "already initialized" in capsys.readouterr().err.lower()

    def test_fails_on_nonexistent_path(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Should fail when the target directory does not exist."""
        bogus = tmp_path / "does-not-exist"
        with pytest.raises(SystemExit) as exc_info:
            app(["init", str(bogus)])
        assert exc_info.value.code == 1
        assert "not a directory" in capsys.readouterr().err.lower()

    def test_json_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """JSON mode emits the expected envelope."""
        target = tmp_path / "jsontest"
        target.mkdir()
        with pytest.raises(SystemExit) as exc_info:
            app(["init", str(target), "--output", "json"])
        assert exc_info.value.code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["command"] == "init"
        assert payload["item"]["repository_directory"] == ".kitaru"

    def test_defaults_to_cwd(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Running without a path argument initializes the working directory."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(SystemExit) as exc_info:
            app(["init"])
        assert exc_info.value.code == 0
        assert (tmp_path / ".kitaru").is_dir() or (tmp_path / ".zen").is_dir()


def test_executions_help_lists_all_supported_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions --help` should show the full execution command surface."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "--help"])
    assert exc_info.value.code == 0
    output = capsys.readouterr().out.lower()
    for command in (
        "get",
        "list",
        "logs",
        "input",
        "replay",
        "retry",
        "resume",
        "cancel",
    ):
        assert command in output


def test_executions_get_renders_execution_details(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions get` should render a detailed execution snapshot."""
    execution = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.WAITING,
        pending_wait=SimpleNamespace(
            name="approve_draft",
            question="Ship this draft?",
        ),
        checkpoints=[
            SimpleNamespace(name="research", status=ExecutionStatus.COMPLETED),
            SimpleNamespace(name="write", status=ExecutionStatus.RUNNING),
        ],
    )
    fake_client = Mock()
    fake_client.executions.get.return_value = execution

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "get", "kr-123"])

    assert exc_info.value.code == 0
    fake_client.executions.get.assert_called_once_with("kr-123")
    output = capsys.readouterr().out
    assert "Kitaru execution" in output
    assert "Execution ID: kr-123" in output
    assert "Flow: content_pipeline" in output
    assert "Status: waiting" in output
    assert "Pending wait: approve_draft" in output
    assert "Wait question: Ship this draft?" in output
    assert "Checkpoints: research (completed), write (running)" in output


def test_executions_list_applies_filters(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions list` should pass filters through to the client API."""
    fake_client = Mock()
    fake_client.executions.list.return_value = [
        _execution_stub(
            exec_id="kr-200",
            flow_name="content_pipeline",
            status=ExecutionStatus.WAITING,
            stack_name="prod",
        ),
        _execution_stub(
            exec_id="kr-199",
            flow_name="content_pipeline",
            status=ExecutionStatus.RUNNING,
            stack_name="prod",
        ),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "list",
                "--status",
                "waiting",
                "--flow",
                "content_pipeline",
                "--limit",
                "5",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.list.assert_called_once_with(
        status="waiting",
        flow="content_pipeline",
        limit=5,
    )
    output = capsys.readouterr().out
    assert "Kitaru executions" in output
    header_lines = [line for line in output.splitlines() if line.strip()]
    assert "ID" in header_lines[1]
    assert "Flow" in header_lines[1]
    assert "Status" in header_lines[1]
    assert "Stack" in header_lines[1]
    assert "kr-200" in output
    assert "content_pipeline" in output
    assert "waiting" in output
    assert "prod" in output


def test_executions_logs_renders_default_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions logs` should print message-only lines by default."""
    fake_client = Mock()
    fake_client.executions.logs.return_value = [
        LogEntry(
            message="Starting research",
            level="INFO",
            timestamp="2026-03-09T10:01:12+00:00",
            source="step",
            checkpoint_name="research",
        ),
        LogEntry(
            message="Writing draft",
            level="INFO",
            timestamp="2026-03-09T10:01:15+00:00",
            source="step",
            checkpoint_name="write",
        ),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123"])

    assert exc_info.value.code == 0
    fake_client.executions.logs.assert_called_once_with(
        "kr-123",
        checkpoint=None,
        source="step",
        limit=None,
    )
    output = capsys.readouterr().out
    assert "Starting research" in output
    assert "Writing draft" in output
    assert "INFO" not in output


def test_executions_logs_supports_verbosity_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`-v` and `-vv` should progressively include more log context."""
    entry = LogEntry(
        message="LLM call completed",
        level="INFO",
        timestamp="2026-03-09T10:01:12+00:00",
        source="step",
        checkpoint_name="research",
        module="research",
    )

    with (
        patch("kitaru.cli.KitaruClient") as client_cls,
        pytest.raises(SystemExit) as exc_info,
    ):
        client_cls.return_value.executions.logs.return_value = [entry]
        app(["executions", "logs", "kr-123", "-v"])

    assert exc_info.value.code == 0
    output_v = capsys.readouterr().out
    assert "2026-03-09 10:01:12" in output_v
    assert "INFO" in output_v
    assert "[research]" not in output_v

    with (
        patch("kitaru.cli.KitaruClient") as client_cls,
        pytest.raises(SystemExit) as exc_info,
    ):
        client_cls.return_value.executions.logs.return_value = [entry]
        app(["executions", "logs", "kr-123", "-vv"])

    assert exc_info.value.code == 0
    output_vv = capsys.readouterr().out
    assert "[research]" in output_vv


def test_executions_logs_grouped_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--grouped` should add checkpoint section headers."""
    fake_client = Mock()
    fake_client.executions.logs.return_value = [
        LogEntry(message="Start", checkpoint_name="research"),
        LogEntry(message="Done", checkpoint_name="write"),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123", "--grouped"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "checkpoint: research" in output
    assert "checkpoint: write" in output


def test_executions_logs_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--output json` should emit a JSON envelope for non-follow mode."""
    fake_client = Mock()
    fake_client.executions.logs.return_value = [
        LogEntry(
            message="Starting research",
            level="INFO",
            timestamp="2026-03-09T10:01:12+00:00",
            source="step",
            checkpoint_name="research",
        )
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123", "--output", "json"])

    assert exc_info.value.code == 0
    stdout = capsys.readouterr().out.strip()
    payload = json.loads(stdout)
    assert payload["command"] == "executions.logs"
    assert payload["count"] == 1
    assert payload["items"][0]["message"] == "Starting research"
    assert payload["items"][0]["checkpoint_name"] == "research"


def test_executions_logs_follow_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--follow --output json` should emit JSONL event objects."""
    running = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.RUNNING,
    )
    completed = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.COMPLETED,
    )
    entry = LogEntry(
        message="Starting research",
        level="INFO",
        timestamp="2026-03-09T10:01:12+00:00",
        checkpoint_name="research",
    )

    fake_client = Mock()
    fake_client.executions.logs.side_effect = [[entry], [entry]]
    fake_client.executions.get.side_effect = [running, completed]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru.cli.time.sleep"),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "logs",
                "kr-123",
                "--follow",
                "--output",
                "json",
                "--interval",
                "0.01",
            ]
        )

    assert exc_info.value.code == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert lines[0]["command"] == "executions.logs"
    assert lines[0]["event"] == "log"
    assert lines[0]["item"]["message"] == "Starting research"
    assert lines[-1]["event"] == "terminal"
    assert lines[-1]["item"]["status"] == "completed"


def test_executions_logs_rejects_invalid_flag_combination(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Grouped text sections are incompatible with JSON output."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "logs",
                "kr-123",
                "--grouped",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "executions.logs"
    assert "cannot be combined" in payload["error"]["message"]


def test_executions_logs_rejects_checkpoint_with_runner_source(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Checkpoint filtering is invalid for runner-level logs."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "logs",
                "kr-123",
                "--source",
                "runner",
                "--checkpoint",
                "research",
            ]
        )

    assert exc_info.value.code == 1
    assert "cannot be combined" in capsys.readouterr().err


def test_executions_logs_empty_state(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An empty result should print a helpful explanatory hint."""
    fake_client = Mock()
    fake_client.executions.logs.return_value = []

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "No log entries found for execution kr-123." in output


def test_executions_logs_follow_until_completion(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--follow` should stream new logs and exit with code 0 on completion."""
    running = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.RUNNING,
    )
    completed = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.COMPLETED,
    )

    first_entry = LogEntry(
        message="Starting research",
        timestamp="2026-03-09T10:01:12+00:00",
        level="INFO",
        checkpoint_name="research",
    )
    second_entry = LogEntry(
        message="Writing draft",
        timestamp="2026-03-09T10:01:15+00:00",
        level="INFO",
        checkpoint_name="write",
    )

    fake_client = Mock()
    fake_client.executions.logs.side_effect = [
        [first_entry],
        [first_entry, second_entry],
    ]
    fake_client.executions.get.side_effect = [running, completed]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru.cli.time.sleep"),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123", "--follow", "--interval", "0.01"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Starting research" in output
    assert "Writing draft" in output
    assert "[Execution completed successfully]" in output


def test_executions_logs_follow_failure_exits_non_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--follow` should exit with code 1 when execution fails."""
    failed = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.FAILED,
        failure=SimpleNamespace(message="Checkpoint failed"),
    )

    fake_client = Mock()
    fake_client.executions.logs.return_value = []
    fake_client.executions.get.return_value = failed

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru.cli.time.sleep"),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123", "--follow", "--interval", "0.01"])

    assert exc_info.value.code == 1
    output = capsys.readouterr().out
    assert "[Execution failed: Checkpoint failed]" in output


def test_executions_logs_follow_failure_shows_retry_hint(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--follow` should show a retry hint when execution fails."""
    failed = _execution_stub(
        exec_id="kr-456",
        flow_name="content_pipeline",
        status=ExecutionStatus.FAILED,
        failure=SimpleNamespace(message="Checkpoint failed"),
    )

    fake_client = Mock()
    fake_client.executions.logs.return_value = []
    fake_client.executions.get.return_value = failed

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru.cli.time.sleep"),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-456", "--follow", "--interval", "0.01"])

    assert exc_info.value.code == 1
    output = capsys.readouterr().out
    assert "kitaru executions retry kr-456" in output
    assert "To retry this failed execution" in output


def test_executions_logs_follow_failure_json_includes_recovery_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--follow --output json` should include recovery_command in terminal event."""
    failed = _execution_stub(
        exec_id="kr-789",
        flow_name="content_pipeline",
        status=ExecutionStatus.FAILED,
        failure=SimpleNamespace(message="Checkpoint failed"),
    )

    fake_client = Mock()
    fake_client.executions.logs.return_value = []
    fake_client.executions.get.return_value = failed

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru.cli.time.sleep"),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "logs",
                "kr-789",
                "--follow",
                "--interval",
                "0.01",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    output = capsys.readouterr().out
    terminal_event = json.loads(output.strip())
    assert terminal_event["event"] == "terminal"
    assert terminal_event["item"]["recovery_command"] == (
        "kitaru executions retry kr-789"
    )


def test_executions_logs_surfaces_backend_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Backend retrieval errors should surface as CLI failures."""
    fake_client = Mock()
    fake_client.executions.logs.side_effect = RuntimeError(
        "Logs for this execution are stored in an OTEL backend."
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "logs", "kr-123"])

    assert exc_info.value.code == 1
    assert "OTEL backend" in capsys.readouterr().err


def _pending_wait_stub(
    *,
    wait_id: str = "wait-001",
    name: str = "approve_deploy",
    question: str | None = "Deploy to prod?",
    schema: dict[str, object] | None = None,
) -> SimpleNamespace:
    """Build a lightweight PendingWait-shaped object for CLI tests."""
    return SimpleNamespace(
        wait_id=wait_id,
        name=name,
        question=question,
        schema=schema,
        metadata={},
        entered_waiting_at=None,
    )


def test_executions_input_parses_json_and_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions input` auto-detects wait and calls client input."""
    fake_client = Mock()
    fake_client.executions.pending_waits.return_value = [
        _pending_wait_stub(wait_id="wait-001", name="approve_deploy"),
    ]
    fake_client.executions.input.return_value = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.WAITING,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "input",
                "kr-123",
                "--value",
                "true",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.input.assert_called_once_with(
        "kr-123",
        wait="wait-001",
        value=True,
    )
    output = capsys.readouterr().out
    assert "Resolved wait input for execution: kr-123" in output
    assert "Status: waiting" in output


def test_executions_input_rejects_invalid_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions input` should fail when `--value` is invalid JSON."""
    fake_client = Mock()
    fake_client.executions.pending_waits.return_value = [
        _pending_wait_stub(),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "input",
                "kr-123",
                "--value",
                "{invalid",
            ]
        )

    assert exc_info.value.code == 1
    assert "Invalid JSON for `--value`" in capsys.readouterr().err


def test_executions_input_json_error_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """JSON mode failures should emit structured errors on stderr."""
    fake_client = Mock()
    fake_client.executions.pending_waits.return_value = [
        _pending_wait_stub(),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "input",
                "kr-123",
                "--value",
                "{invalid",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["command"] == "executions.input"
    assert "Invalid JSON for `--value`" in payload["error"]["message"]


def test_executions_input_requires_exec_id_in_non_interactive(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions input --value true` fails without exec_id."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "input", "--value", "true"])

    assert exc_info.value.code == 1
    assert "Execution ID is required" in capsys.readouterr().err


def test_executions_input_requires_value_or_abort_or_interactive(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions input <id>` fails without --value/--abort/-i."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "input", "kr-123"])

    assert exc_info.value.code == 1
    assert "--value" in capsys.readouterr().err


def test_executions_input_abort_auto_detects_and_aborts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions input <id> --abort` aborts the single wait."""
    fake_client = Mock()
    fake_client.executions.pending_waits.return_value = [
        _pending_wait_stub(wait_id="wait-001"),
    ]
    fake_client.executions.abort_wait.return_value = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.FAILED,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "input", "kr-123", "--abort"])

    assert exc_info.value.code == 0
    fake_client.executions.abort_wait.assert_called_once_with(
        "kr-123",
        wait="wait-001",
    )
    output = capsys.readouterr().out
    assert "Aborted wait for execution: kr-123" in output


def test_executions_input_abort_rejects_value(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--abort` and `--value` are mutually exclusive."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "input",
                "kr-123",
                "--abort",
                "--value",
                "true",
            ]
        )

    assert exc_info.value.code == 1
    assert "--value" in capsys.readouterr().err
    assert "cannot be used with" in capsys.readouterr().err or True


def test_executions_input_multiple_waits_non_interactive_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Multiple pending waits in non-interactive mode should error."""
    fake_client = Mock()
    fake_client.executions.pending_waits.return_value = [
        _pending_wait_stub(wait_id="w1", name="approve"),
        _pending_wait_stub(wait_id="w2", name="review"),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "input", "kr-123", "--value", "true"])

    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "multiple pending waits" in err.lower() or "--interactive" in err


def test_executions_replay_parses_json_and_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions replay` should parse JSON and call replay API."""
    fake_client = Mock()
    fake_client.executions.replay.return_value = _execution_stub(
        exec_id="kr-222",
        flow_name="content_pipeline",
        status=ExecutionStatus.RUNNING,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "replay",
                "kr-111",
                "--from",
                "write_summary",
                "--args",
                '{"topic":"new topic"}',
                "--overrides",
                '{"checkpoint.research":"edited"}',
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.replay.assert_called_once_with(
        "kr-111",
        from_="write_summary",
        overrides={"checkpoint.research": "edited"},
        topic="new topic",
    )
    output = capsys.readouterr().out
    assert "Replayed execution: kr-222" in output
    assert "Status: running" in output


def test_executions_replay_rejects_invalid_overrides_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions replay` should fail when `--overrides` is invalid JSON."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "replay",
                "kr-111",
                "--from",
                "write_summary",
                "--overrides",
                "{invalid",
            ]
        )

    assert exc_info.value.code == 1
    assert "Invalid JSON for `--overrides`" in capsys.readouterr().err


def test_executions_resume_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions resume` should resume and print status details."""
    fake_client = Mock()
    fake_client.executions.resume.return_value = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.RUNNING,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "resume", "kr-123"])

    assert exc_info.value.code == 0
    fake_client.executions.resume.assert_called_once_with("kr-123")
    output = capsys.readouterr().out
    assert "Resumed execution: kr-123" in output
    assert "Status: running" in output


def test_executions_retry_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions retry` should retry and print status details."""
    fake_client = Mock()
    fake_client.executions.retry.return_value = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.RUNNING,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "retry", "kr-123"])

    assert exc_info.value.code == 0
    fake_client.executions.retry.assert_called_once_with("kr-123")
    output = capsys.readouterr().out
    assert "Retried execution: kr-123" in output
    assert "Status: running" in output


def test_executions_cancel_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions cancel` should cancel and print status details."""
    fake_client = Mock()
    fake_client.executions.cancel.return_value = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.CANCELLED,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "cancel", "kr-123"])

    assert exc_info.value.code == 0
    fake_client.executions.cancel.assert_called_once_with("kr-123")
    output = capsys.readouterr().out
    assert "Cancelled execution: kr-123" in output
    assert "Status: cancelled" in output


def test_login_delegates_to_remote_connect(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru login <server>` should delegate to the remote login helper."""
    with (
        patch("kitaru.cli.login_to_server") as mock_login,
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="https://example.com",
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "login",
                "https://example.com/",
                "--api-key",
                "secret-key",
                "--refresh",
                "--project",
                "demo-project",
                "--no-verify-ssl",
                "--timeout",
                "45",
            ]
        )

    assert exc_info.value.code == 0
    mock_login.assert_called_once_with(
        "https://example.com/",
        api_key="secret-key",
        refresh=True,
        project="demo-project",
        no_verify_ssl=True,
        ssl_ca_cert=None,
        timeout=45,
    )

    output = capsys.readouterr().out
    assert "Connected to Kitaru server: https://example.com" in output
    assert "Active project" not in output


def test_login_without_server_starts_local_server(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bare `kitaru login` should start and connect to the local server."""
    with (
        patch(
            "kitaru.cli.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:8383",
                action="started",
            ),
        ) as mock_start,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login"])

    assert exc_info.value.code == 0
    mock_start.assert_called_once_with(port=None, timeout=60)
    output = capsys.readouterr().out
    assert "Starting local Kitaru server..." in output
    assert "Server running at http://127.0.0.1:8383" in output
    assert "Connected to local Kitaru server." in output


def test_login_without_server_reuses_existing_local_server(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bare local login should connect to an already running daemon."""
    with (
        patch(
            "kitaru.cli.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:9090",
                action="connected",
            ),
        ) as mock_start,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login"])

    assert exc_info.value.code == 0
    mock_start.assert_called_once_with(port=None, timeout=60)
    output = capsys.readouterr().out
    assert "Server already running at http://127.0.0.1:9090" in output
    assert "Connected to local Kitaru server." in output


def test_login_without_server_restarts_local_server_on_explicit_port(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Explicit `--port` should restart the local daemon on that port."""
    with (
        patch(
            "kitaru.cli.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:9090",
                action="restarted",
            ),
        ) as mock_start,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login", "--port", "9090"])

    assert exc_info.value.code == 0
    mock_start.assert_called_once_with(port=9090, timeout=60)
    output = capsys.readouterr().out
    assert "Restarting local Kitaru server on port 9090..." in output
    assert "Server running at http://127.0.0.1:9090" in output
    assert "Connected to local Kitaru server." in output


def test_login_surfaces_validation_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid server URLs should exit cleanly with a helpful error."""
    with pytest.raises(SystemExit) as exc_info:
        app(["login", "example.com"])

    assert exc_info.value.code == 1
    assert "Invalid Kitaru server URL" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["login", "--api-key", "secret"], "--api-key is only used"),
        (["login", "--project", "demo"], "--project is only used"),
        (["login", "--refresh"], "--refresh is only used"),
        (["login", "--no-verify-ssl"], "--no-verify-ssl is only used"),
        (["login", "--ssl-ca-cert", "/tmp/ca.pem"], "--ssl-ca-cert is only used"),
    ],
)
def test_login_rejects_remote_only_flags_without_server(
    args: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bare local login should reject remote-only flags."""
    with pytest.raises(SystemExit) as exc_info:
        app(args)

    assert exc_info.value.code == 1
    assert message in capsys.readouterr().err


def test_login_rejects_port_with_remote_server(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Remote login should reject the local-only `--port` flag."""
    with pytest.raises(SystemExit) as exc_info:
        app(["login", "https://example.com", "--port", "9090"])

    assert exc_info.value.code == 1
    assert "--port is only used for local server startup." in capsys.readouterr().err


def test_login_rejects_auth_environment_overrides(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Login should fail fast if ZenML auth env vars are already driving auth."""
    monkeypatch.setenv("ZENML_STORE_URL", "https://env.example.com")

    with pytest.raises(SystemExit) as exc_info:
        app(["login", "https://example.com"])

    assert exc_info.value.code == 1
    assert (
        "cannot override existing auth environment variables" in capsys.readouterr().err
    )


def test_login_rejects_kitaru_auth_environment_overrides(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Login should report public KITARU auth vars when they drive auth."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://env.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")

    with pytest.raises(SystemExit) as exc_info:
        app(["login", "https://example.com"])

    assert exc_info.value.code == 1
    error_output = capsys.readouterr().err
    assert "KITARU_SERVER_URL" in error_output
    assert "KITARU_AUTH_TOKEN" in error_output
    assert "ZENML_STORE_URL" not in error_output


def test_local_login_warns_for_auth_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Local login should warn but proceed when auth env vars are present."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://env.example.com")

    with (
        patch(
            "kitaru.cli.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:8383",
                action="started",
            ),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Auth environment variables are active (KITARU_SERVER_URL)." in output
    assert "runtime connections may still use those environment variables" in output
    assert "Connected to local Kitaru server." in output


def test_local_login_warns_when_switching_from_remote(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bare local login should warn when it disconnects from a remote target."""
    with (
        patch(
            "kitaru.cli.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:8383",
                action="started",
            ),
        ),
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="https://prod.kitaru.io",
        ),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Disconnecting from remote server: https://prod.kitaru.io" in output
    assert "Connected to local Kitaru server." in output


def test_logout_rejects_kitaru_auth_environment_overrides(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Logout should also refuse to fight active KITARU auth env vars."""
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")

    with pytest.raises(SystemExit) as exc_info:
        app(["logout"])

    assert exc_info.value.code == 1
    assert "KITARU_AUTH_TOKEN" in capsys.readouterr().err


def test_logout_resets_remote_connection() -> None:
    """Remote logout should also stop any registered local daemon."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="https://example.com/")
    fake_credentials_store = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch(
            "kitaru.cli._get_connected_server_url", return_value="https://example.com"
        ),
        patch(
            "kitaru.cli.stop_registered_local_server",
            return_value=SimpleNamespace(
                stopped=True,
                url="http://127.0.0.1:8383",
            ),
        ),
        patch(
            "kitaru.cli.get_credentials_store",
            return_value=fake_credentials_store,
        ),
    ):
        result = _logout_current_connection()

    fake_gc.set_default_store.assert_called_once_with()
    fake_credentials_store.clear_credentials.assert_called_once_with(
        "https://example.com"
    )
    assert result.mode == "remote_server"
    assert result.local_server_stopped is True
    assert str(result) == (
        "Logged out from Kitaru server: https://example.com\n"
        "Stopped local server (port 8383)."
    )


def test_logout_returns_local_server_mode_for_local_connection() -> None:
    """Local logout should report local-server mode and stop the daemon."""
    fake_gc = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=True),
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="http://127.0.0.1:8383",
        ),
        patch(
            "kitaru.cli.stop_registered_local_server",
            return_value=SimpleNamespace(
                stopped=True,
                url="http://127.0.0.1:8383",
            ),
        ),
    ):
        result = _logout_current_connection()

    assert result.mode == "local_server"
    assert result.local_server_stopped is True
    assert str(result) == "Logged out from the local Kitaru server."


def test_logout_treats_localhost_docker_server_as_remote() -> None:
    """Docker localhost URLs should not be mistaken for the local daemon."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="http://localhost:8080")
    fake_credentials_store = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="http://localhost:8080",
        ),
        patch(
            "kitaru.cli.stop_registered_local_server",
            return_value=SimpleNamespace(
                stopped=True,
                url="http://127.0.0.1:8383",
            ),
        ),
        patch(
            "kitaru.cli.get_credentials_store",
            return_value=fake_credentials_store,
        ),
        patch(
            "kitaru.inspection.get_local_server",
            return_value=SimpleNamespace(
                status=SimpleNamespace(url="http://127.0.0.1:8383"),
                config=SimpleNamespace(url="http://127.0.0.1:8383"),
            ),
        ),
    ):
        result = _logout_current_connection()

    fake_gc.set_default_store.assert_called_once_with()
    fake_credentials_store.clear_credentials.assert_called_once_with(
        "http://localhost:8080"
    )
    assert result.mode == "remote_server"
    assert result.target == "http://localhost:8080"


def test_logout_is_idempotent_on_local_store() -> None:
    """The logout helper should be a no-op when already on the local store."""
    fake_gc = Mock()
    fake_gc.uses_local_store = True

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch("kitaru.cli._get_connected_server_url", return_value=None),
        patch(
            "kitaru.cli.stop_registered_local_server",
            return_value=SimpleNamespace(stopped=False, url=None),
        ),
    ):
        result = _logout_current_connection()

    assert str(result) == "Kitaru is already using its local default store."


def test_logout_clears_remote_store_when_local_fallback_is_missing() -> None:
    """Logout should still clear stale localhost state without local fallback."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="http://127.0.0.1:8237")
    fake_gc.set_default_store.side_effect = ImportError("sqlalchemy missing")
    fake_credentials_store = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="http://127.0.0.1:8237",
        ),
        patch(
            "kitaru.cli.stop_registered_local_server",
            return_value=SimpleNamespace(
                stopped=True,
                url="http://127.0.0.1:8237",
            ),
        ),
        patch(
            "kitaru.cli.get_credentials_store",
            return_value=fake_credentials_store,
        ),
        patch(
            "kitaru.inspection.get_local_server",
            return_value=SimpleNamespace(
                status=SimpleNamespace(url=None),
                config=SimpleNamespace(url=None, port=8237, ip_address="127.0.0.1"),
            ),
        ),
    ):
        result = _logout_current_connection()

    fake_gc._write_config.assert_called_once_with()
    fake_credentials_store.clear_credentials.assert_not_called()
    assert result.mode == "local_server"
    assert result.local_server_stopped is True


def test_log_store_set_delegates_to_config(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru log-store set` delegates persistence to config helpers."""
    with (
        patch("kitaru.cli.set_global_log_store") as mock_set,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_set.return_value = SimpleNamespace(
            backend="datadog",
            endpoint="https://logs.datadoghq.com",
            api_key="{{ DATADOG_KEY }}",
            source="global user config",
        )
        app(
            [
                "log-store",
                "set",
                "datadog",
                "--endpoint",
                "https://logs.datadoghq.com",
                "--api-key",
                "{{ DATADOG_KEY }}",
            ]
        )

    assert exc_info.value.code == 0
    mock_set.assert_called_once_with(
        "datadog",
        endpoint="https://logs.datadoghq.com",
        api_key="{{ DATADOG_KEY }}",
    )
    output = capsys.readouterr().out
    assert "Saved global log-store override." in output
    assert "Effective backend: datadog" in output


def test_log_store_show_renders_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru log-store show` prints the resolved backend snapshot."""
    with (
        patch("kitaru.cli.resolve_log_store") as mock_resolve,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_resolve.return_value = SimpleNamespace(
            backend="datadog",
            endpoint="https://logs.datadoghq.com",
            api_key="top-secret",
            source="environment",
        )
        app(["log-store", "show"])

    assert exc_info.value.code == 0
    mock_resolve.assert_called_once_with()
    output = capsys.readouterr().out
    assert "Kitaru log store" in output
    assert "Backend: datadog" in output
    assert "Endpoint: https://logs.datadoghq.com" in output
    assert "API key: configured" in output
    assert "top-secret" not in output
    assert "Source: environment" in output


def test_log_store_show_warns_on_stack_mismatch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru log-store show` should explain preference vs active-stack mismatch."""
    with (
        patch("kitaru.cli.resolve_log_store") as mock_resolve,
        patch(
            "kitaru.cli._log_store_mismatch_details",
            return_value=(
                "datadog (preferred) ⚠ stack uses artifact-store",
                "Active stack uses: artifact-store (stack: local)\n"
                "The Kitaru log-store preference is not wired into stack "
                "selection yet.",
            ),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_resolve.return_value = SimpleNamespace(
            backend="datadog",
            endpoint="https://logs.datadoghq.com",
            api_key="top-secret",
            source="global user config",
        )
        app(["log-store", "show"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Active stack uses: artifact-store (stack: local)" in output
    assert "not wired into stack selection yet" in output


def test_log_store_set_reports_environment_override(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Set should explain when environment settings still win."""
    with (
        patch("kitaru.cli.set_global_log_store") as mock_set,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_set.return_value = SimpleNamespace(
            backend="honeycomb",
            endpoint="https://api.honeycomb.io",
            api_key="env-secret",
            source="environment",
        )
        app(
            [
                "log-store",
                "set",
                "datadog",
                "--endpoint",
                "https://logs.datadoghq.com",
            ]
        )

    assert exc_info.value.code == 0
    mock_set.assert_called_once_with(
        "datadog",
        endpoint="https://logs.datadoghq.com",
        api_key=None,
    )
    output = capsys.readouterr().out
    assert "Saved global log-store override." in output
    assert "Effective backend: honeycomb (from environment settings)" in output


def test_log_store_reset_clears_override(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru log-store reset` clears persisted log-store override state."""
    with (
        patch("kitaru.cli.reset_global_log_store") as mock_reset,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_reset.return_value = SimpleNamespace(
            backend="artifact-store",
            endpoint=None,
            api_key=None,
            source="default",
        )
        app(["log-store", "reset"])

    assert exc_info.value.code == 0
    mock_reset.assert_called_once_with()
    output = capsys.readouterr().out
    assert "Cleared global log-store override." in output
    assert "Effective backend: artifact-store (from default settings)" in output


def test_log_store_reset_reports_environment_override(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Reset should explain when env settings still override persisted config."""
    with (
        patch("kitaru.cli.reset_global_log_store") as mock_reset,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_reset.return_value = SimpleNamespace(
            backend="datadog",
            endpoint="https://logs.datadoghq.com",
            api_key="env-secret",
            source="environment",
        )
        app(["log-store", "reset"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Cleared global log-store override." in output
    assert "Effective backend: datadog (from environment settings)" in output


def test_log_store_set_surfaces_validation_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid log-store settings should exit with a user-friendly error."""
    with (
        patch(
            "kitaru.cli.set_global_log_store",
            side_effect=ValueError("Invalid log-store endpoint"),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "log-store",
                "set",
                "datadog",
                "--endpoint",
                "not-a-url",
            ]
        )

    assert exc_info.value.code == 1
    assert "Invalid log-store endpoint" in capsys.readouterr().err


def test_parse_secret_assignments_accepts_equals_and_split_values() -> None:
    """Secrets assignment parsing should support `--KEY=value` and split forms."""
    parsed = _parse_secret_assignments(
        [
            "--OPENAI_API_KEY=sk-123",
            "--ANTHROPIC_API_KEY",
            "sk-ant-456",
        ]
    )

    assert parsed == {
        "OPENAI_API_KEY": "sk-123",
        "ANTHROPIC_API_KEY": "sk-ant-456",
    }


def test_parse_secret_assignments_rejects_invalid_keys() -> None:
    """Secrets assignment parsing should reject non env-var key names."""
    with pytest.raises(ValueError, match="Invalid secret key"):
        _parse_secret_assignments(["--OPENAI-API-KEY=sk-123"])


def test_parse_secret_assignments_rejects_duplicate_keys() -> None:
    """Duplicate secret keys in one command should fail fast."""
    with pytest.raises(ValueError, match="Duplicate secret key"):
        _parse_secret_assignments(
            [
                "--OPENAI_API_KEY=sk-123",
                "--OPENAI_API_KEY=sk-456",
            ]
        )


def test_parse_secret_assignments_rejects_empty_payload() -> None:
    """A bare separator token should still fail with no parsed assignments."""
    with pytest.raises(ValueError, match="Provide at least one secret assignment"):
        _parse_secret_assignments(["--"])


def test_parse_secret_assignments_rejects_missing_split_value() -> None:
    """Split assignment values cannot be another assignment token."""
    with pytest.raises(ValueError, match="Missing value for secret key"):
        _parse_secret_assignments(
            [
                "--OPENAI_API_KEY",
                "--ANTHROPIC_API_KEY=sk-ant-123",
            ]
        )


def test_model_register_persists_alias(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru model register` should save aliases with optional secret links."""
    with (
        patch("kitaru.cli._resolve_secret_exact") as mock_resolve_secret,
        patch("kitaru.cli.register_model_alias") as mock_register,
        patch("kitaru.cli.Client") as mock_client,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_register.return_value = SimpleNamespace(
            alias="fast",
            model="openai/gpt-4o-mini",
            secret="openai-creds",
            is_default=True,
        )
        app(
            [
                "model",
                "register",
                "fast",
                "--model",
                "openai/gpt-4o-mini",
                "--secret",
                "openai-creds",
            ]
        )

    assert exc_info.value.code == 0
    mock_resolve_secret.assert_called_once()
    assert mock_resolve_secret.call_args.args[1] == "openai-creds"
    mock_register.assert_called_once_with(
        "fast",
        model="openai/gpt-4o-mini",
        secret="openai-creds",
    )
    mock_client.assert_called_once_with()
    output = capsys.readouterr().out
    assert "Saved model alias: fast" in output
    assert "Model: openai/gpt-4o-mini" in output
    assert "Secret: openai-creds" in output
    assert "Default alias" in output


def test_model_register_works_without_secret(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru model register` should support plain aliases without secret refs."""
    with (
        patch("kitaru.cli._resolve_secret_exact") as mock_resolve_secret,
        patch("kitaru.cli.register_model_alias") as mock_register,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_register.return_value = SimpleNamespace(
            alias="smart",
            model="anthropic/claude-sonnet-4-20250514",
            secret=None,
            is_default=False,
        )
        app(
            [
                "model",
                "register",
                "smart",
                "--model",
                "anthropic/claude-sonnet-4-20250514",
            ]
        )

    assert exc_info.value.code == 0
    mock_resolve_secret.assert_not_called()
    mock_register.assert_called_once_with(
        "smart",
        model="anthropic/claude-sonnet-4-20250514",
        secret=None,
    )
    output = capsys.readouterr().out
    assert "Saved model alias: smart" in output


def test_model_list_renders_aliases(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru model list` should render aliases in a snapshot view."""
    with (
        patch("kitaru.cli.list_model_aliases") as mock_list_models,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_models.return_value = [
            SimpleNamespace(
                alias="fast",
                model="openai/gpt-4o-mini",
                secret="openai-creds",
                is_default=True,
            ),
            SimpleNamespace(
                alias="smart",
                model="anthropic/claude-sonnet-4-20250514",
                secret=None,
                is_default=False,
            ),
        ]
        app(["model", "list"])

    assert exc_info.value.code == 0
    mock_list_models.assert_called_once_with()
    output = capsys.readouterr().out
    assert "Kitaru models" in output
    assert "fast: openai/gpt-4o-mini (secret=openai-creds) [default]" in output
    assert "smart: anthropic/claude-sonnet-4-20250514" in output


def test_model_list_renders_empty_state(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru model list` should show a helpful empty-state message."""
    with (
        patch("kitaru.cli.list_model_aliases", return_value=[]),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["model", "list"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru models" in output
    assert "Models: none found" in output


def test_model_list_reads_transported_registry(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`kitaru model list` should reflect aliases from KITARU_MODEL_REGISTRY."""
    monkeypatch.setenv(
        KITARU_MODEL_REGISTRY_ENV,
        ModelRegistryConfig(
            aliases={
                "fast": ModelAliasConfig(
                    model="openai/gpt-4o-mini",
                    secret="openai-creds",
                )
            },
            default="fast",
        ).model_dump_json(exclude_none=True),
    )

    with pytest.raises(SystemExit) as exc_info:
        app(["model", "list"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru models" in output
    assert "fast: openai/gpt-4o-mini (secret=openai-creds) [default]" in output


def test_secrets_set_creates_secret(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets set` should create private secrets by default."""
    fake_client = Mock()
    fake_client.create_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "secrets",
                "set",
                "openai-creds",
                "--OPENAI_API_KEY=sk-123",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.create_secret.assert_called_once_with(
        name="openai-creds",
        values={"OPENAI_API_KEY": "sk-123"},
        private=True,
    )
    output = capsys.readouterr().out
    assert "Created secret: openai-creds" in output
    assert "Secret ID: secret-id" in output


def test_secrets_set_updates_existing_secret(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets set` should update existing secrets when already present."""
    fake_client = Mock()
    fake_client.create_secret.side_effect = EntityExistsError("already exists")
    fake_client.get_secret.return_value = SimpleNamespace(id="secret-id")
    fake_client.update_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "secrets",
                "set",
                "openai-creds",
                "--OPENAI_API_KEY=sk-123",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.get_secret.assert_called_once_with(
        name_id_or_prefix="openai-creds",
        allow_partial_name_match=False,
        allow_partial_id_match=False,
    )
    fake_client.update_secret.assert_called_once_with(
        name_id_or_prefix="secret-id",
        add_or_update_values={"OPENAI_API_KEY": "sk-123"},
    )
    output = capsys.readouterr().out
    assert "Updated secret: openai-creds" in output


def test_secrets_set_rejects_invalid_assignments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Secrets set should fail with a helpful error for invalid assignments."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "secrets",
                "set",
                "openai-creds",
                "OPENAI_API_KEY=sk-123",
            ]
        )

    assert exc_info.value.code == 1
    assert "Invalid secret assignment" in capsys.readouterr().err


def test_secrets_set_json_output_accepts_output_after_assignments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets set` should accept `--output json` after assignments."""
    fake_client = Mock()
    fake_client.create_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=True,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
        secret_values={"OPENAI_API_KEY": "sk-123"},
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "secrets",
                "set",
                "openai-creds",
                "--OPENAI_API_KEY=sk-123",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "secrets.set"
    assert payload["item"]["name"] == "openai-creds"
    assert payload["item"]["result"] == "created"


def test_secrets_show_hides_values_by_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets show` should not render raw values unless requested."""
    fake_secret = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=True,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
        secret_values={"OPENAI_API_KEY": "sk-123"},
    )
    fake_client = Mock()
    fake_client.get_secret.return_value = fake_secret

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "show", "openai-creds"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru secret" in output
    assert "Name: openai-creds" in output
    assert "Visibility: private" in output
    assert "Keys: OPENAI_API_KEY" in output
    assert "sk-123" not in output


def test_secrets_show_displays_values_when_requested(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets show --show-values` should print value rows."""
    fake_secret = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=True,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
        secret_values={"OPENAI_API_KEY": "sk-123"},
    )
    fake_client = Mock()
    fake_client.get_secret.return_value = fake_secret

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "show", "openai-creds", "--show-values"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Value (OPENAI_API_KEY): sk-123" in output


def test_secrets_list_renders_all_pages_sorted(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets list` should merge all pages and sort by secret name."""
    secret_z = SimpleNamespace(name="zeta", id="secret-z", private=False)
    secret_a = SimpleNamespace(name="alpha", id="secret-a", private=True)
    fake_client = Mock()
    fake_client.list_secrets.side_effect = [
        SimpleNamespace(items=[secret_z], total_pages=2, max_size=1),
        SimpleNamespace(items=[secret_a], total_pages=2, max_size=1),
    ]

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "list"])

    assert exc_info.value.code == 0
    fake_client.list_secrets.assert_has_calls(
        [
            call(page=1),
            call(page=2, size=1),
        ]
    )
    output = capsys.readouterr().out
    assert "Kitaru secrets" in output
    assert "alpha: secret-a (private)" in output
    assert "zeta: secret-z (public)" in output
    assert output.index("alpha: secret-a (private)") < output.index(
        "zeta: secret-z (public)"
    )


def test_secrets_list_surfaces_client_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets list` should surface backend errors as CLI errors."""
    with (
        patch("kitaru.cli.Client", side_effect=RuntimeError("offline")),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "list"])

    assert exc_info.value.code == 1
    assert "offline" in capsys.readouterr().err


def test_secrets_delete_resolves_exact_secret_before_deleting(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets delete` should resolve exact secret and delete by ID."""
    fake_client = Mock()
    fake_client.get_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "delete", "openai-creds"])

    assert exc_info.value.code == 0
    fake_client.get_secret.assert_called_once_with(
        name_id_or_prefix="openai-creds",
        allow_partial_name_match=False,
        allow_partial_id_match=False,
    )
    fake_client.delete_secret.assert_called_once_with(name_id_or_prefix="secret-id")
    output = capsys.readouterr().out
    assert "Deleted secret: openai-creds" in output


def test_secrets_delete_surfaces_backend_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Delete should map backend failures to a user-facing CLI error."""
    fake_client = Mock()
    fake_client.get_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
    )
    fake_client.delete_secret.side_effect = KeyError("already deleted")

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "delete", "openai-creds"])

    assert exc_info.value.code == 1
    assert "already deleted" in capsys.readouterr().err


def test_stack_list_renders_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack list` should render visible stacks and active marker."""
    with (
        patch("kitaru.cli.get_available_stacks") as mock_list_stacks,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_stacks.return_value = [
            SimpleNamespace(id="stack-local-id", name="local", is_active=False),
            SimpleNamespace(id="stack-prod-id", name="prod", is_active=True),
        ]
        app(["stack", "list"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru stacks" in output
    assert "local: stack-local-id" in output
    assert "prod: stack-prod-id (active)" in output


def test_stack_current_renders_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack current` should show active stack details."""
    with (
        patch("kitaru.cli.get_current_stack") as mock_current_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_current_stack.return_value = SimpleNamespace(
            id="stack-prod-id",
            name="prod",
            is_active=True,
        )
        app(["stack", "current"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru stack" in output
    assert "Active stack: prod" in output
    assert "Stack ID: stack-prod-id" in output


def test_stack_show_renders_translated_component_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack show` should render Kitaru component labels and details."""
    with (
        patch("kitaru.cli._show_stack_operation") as mock_show_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_show_stack.return_value = _stack_details_stub()
        app(["stack", "show", "my-k8s"])

    assert exc_info.value.code == 0
    mock_show_stack.assert_called_once_with("my-k8s")
    output = capsys.readouterr().out
    assert "Kitaru stack" in output
    assert "Name: my-k8s" in output
    assert "Type: kubernetes" in output
    assert "Managed: yes" in output
    assert "Runner: my-k8s-runner (kubernetes)" in output
    assert "cluster: demo-cluster" in output
    assert "Storage: my-k8s-storage (s3); location: s3://bucket/kitaru" in output
    assert (
        "Image registry: my-k8s-registry (aws); location: "
        "123456789012.dkr.ecr.us-east-1.amazonaws.com" in output
    )
    assert "artifact_store" not in output
    assert "container_registry" not in output


def test_stack_show_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack show --output json` should emit translated stack details."""
    with (
        patch("kitaru.cli._show_stack_operation") as mock_show_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_show_stack.return_value = _stack_details_stub()
        app(["stack", "show", "my-k8s", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.show",
        "item": {
            "id": "stack-my-k8s-id",
            "name": "my-k8s",
            "is_active": True,
            "is_managed": True,
            "stack_type": "kubernetes",
            "components": [
                {
                    "role": "runner",
                    "name": "my-k8s-runner",
                    "backend": "kubernetes",
                    "details": {
                        "cluster": "demo-cluster",
                        "region": "us-east-1",
                        "namespace": "default",
                    },
                },
                {
                    "role": "storage",
                    "name": "my-k8s-storage",
                    "backend": "s3",
                    "details": {
                        "location": "s3://bucket/kitaru",
                    },
                },
                {
                    "role": "image_registry",
                    "name": "my-k8s-registry",
                    "backend": "aws",
                    "details": {
                        "location": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                    },
                },
            ],
        },
    }


def test_stack_show_surfaces_structured_json_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`stack show` should reuse the standard JSON error envelope."""
    with (
        patch(
            "kitaru.cli._show_stack_operation",
            side_effect=ValueError("Stack 'ghost' not found."),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["stack", "show", "ghost", "--output", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "command": "stack.show",
        "error": {
            "message": "Stack 'ghost' not found.",
            "type": "ValueError",
        },
    }


def test_stack_use_delegates_to_config(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack use` should activate and report the selected stack."""
    with (
        patch("kitaru.cli.set_active_stack") as mock_use_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_use_stack.return_value = SimpleNamespace(
            id="stack-prod-id",
            name="prod",
            is_active=True,
        )
        app(["stack", "use", "prod"])

    assert exc_info.value.code == 0
    mock_use_stack.assert_called_once_with("prod")
    output = capsys.readouterr().out
    assert "Activated stack: prod" in output
    assert "Stack ID: stack-prod-id" in output


def test_stack_use_surfaces_validation_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Stack validation errors should surface as CLI-friendly failures."""
    with (
        patch(
            "kitaru.cli.set_active_stack",
            side_effect=ValueError("Stack name or ID cannot be empty."),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["stack", "use", "prod"])

    assert exc_info.value.code == 1
    assert "Stack name or ID cannot be empty." in capsys.readouterr().err


def test_stack_create_reports_auto_activation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack create` should report creation and auto-activation."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub()
        app(["stack", "create", "dev"])

    assert exc_info.value.code == 0
    mock_create_stack.assert_called_once_with(
        "dev",
        stack_type=StackType.LOCAL,
        activate=True,
        remote_spec=None,
    )
    output = capsys.readouterr().out
    assert "Created stack: dev" in output
    assert "Active stack: default → dev" in output


def test_stack_create_no_activate_skips_active_stack_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack create --no-activate` should not print an activation line."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            is_active=False,
            previous_active_stack=None,
        )
        app(["stack", "create", "dev", "--no-activate"])

    assert exc_info.value.code == 0
    mock_create_stack.assert_called_once_with(
        "dev",
        stack_type=StackType.LOCAL,
        activate=False,
        remote_spec=None,
    )
    output = capsys.readouterr().out
    assert "Created stack: dev" in output
    assert "Active stack:" not in output


def test_stack_create_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru stack create --output json` should emit operation metadata."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub()
        app(["stack", "create", "dev", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.create",
        "item": {
            "id": "stack-dev-id",
            "name": "dev",
            "is_active": True,
            "previous_active_stack": "default",
            "components_created": [
                "dev (orchestrator)",
                "dev (artifact_store)",
            ],
            "stack_type": "local",
        },
    }


def test_stack_create_rejects_kubernetes_flags_for_local_stack(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Local stack creation should reject remote-stack flags."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--artifact-store", "s3://bucket/kitaru"])

    assert exc_info.value.code == 1
    assert (
        "Remote stack options require --type kubernetes, --type vertex, "
        "--type sagemaker, or --type azureml: --artifact-store"
        in capsys.readouterr().err
    )


def test_stack_create_rejects_blank_kubernetes_flags_for_local_stack(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Blank remote-stack flag values still count as explicit local-stack inputs."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--artifact-store", "   "])

    assert exc_info.value.code == 1
    assert (
        "Remote stack options require --type kubernetes, --type vertex, "
        "--type sagemaker, or --type azureml: --artifact-store"
        in capsys.readouterr().err
    )


def test_stack_create_kubernetes_requires_all_mandatory_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Kubernetes stack creation should report all missing required flags."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--type", "kubernetes"])

    assert exc_info.value.code == 1
    assert (
        "--type kubernetes requires: --artifact-store, --container-registry, "
        "--cluster, --region."
    ) in capsys.readouterr().err


def test_stack_create_vertex_requires_all_mandatory_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Vertex stack creation should report all missing required flags."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--type", "vertex"])

    assert exc_info.value.code == 1
    assert (
        "--type vertex requires: --artifact-store, --container-registry, --region."
    ) in capsys.readouterr().err


def test_stack_create_sagemaker_requires_all_mandatory_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SageMaker stack creation should report all missing required flags."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--type", "sagemaker"])

    assert exc_info.value.code == 1
    assert (
        "--type sagemaker requires: --artifact-store, --container-registry, "
        "--region, --execution-role."
    ) in capsys.readouterr().err


def test_stack_create_azureml_requires_all_mandatory_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AzureML stack creation should report all missing required flags."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--type", "azureml"])

    assert exc_info.value.code == 1
    assert (
        "--type azureml requires: --artifact-store, --container-registry, "
        "--subscription-id, --resource-group, --workspace."
    ) in capsys.readouterr().err


def test_stack_create_vertex_rejects_kubernetes_only_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Vertex stack creation should still reject Kubernetes-only inputs."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "vertex-dev",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
                "--cluster",
                "demo-gke",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "Kubernetes-only options require --type kubernetes: --cluster"
        in capsys.readouterr().err
    )


def test_stack_create_azureml_rejects_kubernetes_only_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AzureML stack creation should still reject Kubernetes-only inputs."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "azure-dev",
                "--type",
                "azureml",
                "--artifact-store",
                "az://container/kitaru",
                "--container-registry",
                "demo.azurecr.io/team/image",
                "--subscription-id",
                "00000000-0000-0000-0000-000000000123",
                "--resource-group",
                "rg-demo",
                "--workspace",
                "ws-demo",
                "--cluster",
                "demo-aks",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "Kubernetes-only options require --type kubernetes: --cluster"
        in capsys.readouterr().err
    )


def test_stack_create_local_rejects_sagemaker_only_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Non-SageMaker stack creation should reject SageMaker-only inputs."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "dev",
                "--execution-role",
                "arn:aws:iam::123456789012:role/SageMakerRole",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "SageMaker-only options require --type sagemaker: --execution-role"
        in capsys.readouterr().err
    )


def test_stack_create_local_rejects_azureml_only_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Non-AzureML stack creation should reject Azure-only inputs."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "dev",
                "--subscription-id",
                "00000000-0000-0000-0000-000000000123",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "AzureML-only options require --type azureml: --subscription-id"
        in capsys.readouterr().err
    )


def test_stack_create_rejects_unsupported_stack_type_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid stack types should use the structured JSON error contract."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--type", "modal", "--output", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "command": "stack.create",
        "error": {
            "message": (
                "Unsupported stack type: modal. Use 'local', "
                "'kubernetes', 'vertex', 'sagemaker', or 'azureml'."
            ),
            "type": "ValueError",
        },
    }


def test_stack_create_rejects_blank_type_override(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """An explicit blank --type should fail instead of silently defaulting to local."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-k8s
type: kubernetes
artifact_store: s3://bucket/kitaru
container_registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
cluster: demo-cluster
region: us-east-1
""".strip(),
    )

    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "--file", str(stack_file), "--type", ""])

    assert exc_info.value.code == 1
    assert (
        "Unsupported stack type: . Use 'local', 'kubernetes', 'vertex', "
        "'sagemaker', or 'azureml'." in capsys.readouterr().err
    )


def test_stack_create_kubernetes_rejects_unsupported_artifact_store_scheme(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Provider inference should reject unsupported artifact-store schemes."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "dev",
                "--type",
                "kubernetes",
                "--artifact-store",
                "az://bucket/kitaru",
                "--container-registry",
                "registry.example.com/repo",
                "--cluster",
                "demo-cluster",
                "--region",
                "westeurope",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "Cannot infer cloud provider from 'az://bucket/kitaru'. "
        "Use an s3:// or gs:// URI."
    ) in capsys.readouterr().err


def test_stack_create_azureml_rejects_non_azure_artifact_store(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AzureML stack creation should require an Azure artifact-store URI."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "azure-dev",
                "--type",
                "azureml",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "demo.azurecr.io/team/image",
                "--subscription-id",
                "00000000-0000-0000-0000-000000000123",
                "--resource-group",
                "rg-demo",
                "--workspace",
                "ws-demo",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "AzureML stacks require an az://, abfs://, or abfss:// artifact store "
        "URI. Received: 's3://bucket/kitaru'."
    ) in capsys.readouterr().err


def test_stack_create_kubernetes_builds_aws_spec() -> None:
    """AWS-backed Kubernetes stacks should infer provider and defaults."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-k8s",
            stack_type="kubernetes",
            resources={
                "provider": "aws",
                "cluster": "demo-cluster",
                "region": "us-east-1",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-k8s",
                "--type",
                "kubernetes",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--cluster",
                "demo-cluster",
                "--region",
                "us-east-1",
            ]
        )

    assert exc_info.value.code == 0
    mock_create_stack.assert_called_once()
    assert mock_create_stack.call_args.args == ("my-k8s",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.KUBERNETES
    assert mock_create_stack.call_args.kwargs["activate"] is True
    kubernetes_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(kubernetes_spec, KubernetesStackSpec)
    assert kubernetes_spec.model_dump(mode="json") == {
        "provider": "aws",
        "artifact_store": "s3://bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "cluster": "demo-cluster",
        "region": "us-east-1",
        "namespace": "default",
        "credentials": None,
        "verify": True,
    }


def test_stack_create_kubernetes_builds_gcp_spec_with_credentials_and_no_verify() -> (
    None
):
    """GCP-backed Kubernetes stacks should preserve raw credentials and verify flag."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-k8s",
            stack_type="kubernetes",
            resources={
                "provider": "gcp",
                "cluster": "demo-cluster",
                "region": "us-central1",
                "artifact_store": "gs://bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/demo/repo",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-k8s",
                "--type",
                "kubernetes",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--cluster",
                "demo-cluster",
                "--region",
                "us-central1",
                "--namespace",
                "agents",
                "--credentials",
                "gcp-service-account:/tmp/key.json",
                "--no-verify",
            ]
        )

    assert exc_info.value.code == 0
    kubernetes_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(kubernetes_spec, KubernetesStackSpec)
    assert kubernetes_spec.model_dump(mode="json") == {
        "provider": "gcp",
        "artifact_store": "gs://bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/demo/repo",
        "cluster": "demo-cluster",
        "region": "us-central1",
        "namespace": "agents",
        "credentials": "gcp-service-account:/tmp/key.json",
        "verify": False,
    }


def test_stack_create_vertex_builds_gcp_spec() -> None:
    """Vertex stacks should build the shared Vertex spec without Kubernetes fields."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-vertex",
            stack_type="vertex",
            resources={
                "provider": "gcp",
                "region": "us-central1",
                "artifact_store": "gs://bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/demo/repo",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-vertex",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
                "--credentials",
                "gcp-service-account:/tmp/key.json",
                "--no-verify",
            ]
        )

    assert exc_info.value.code == 0
    vertex_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(vertex_spec, VertexStackSpec)
    assert vertex_spec.model_dump(mode="json") == {
        "artifact_store": "gs://bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/demo/repo",
        "region": "us-central1",
        "credentials": "gcp-service-account:/tmp/key.json",
        "verify": False,
    }


def test_stack_create_sagemaker_builds_aws_spec() -> None:
    """SageMaker stacks should build the shared spec without cluster fields."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-sagemaker",
            stack_type="sagemaker",
            resources={
                "provider": "aws",
                "region": "us-east-1",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-sagemaker",
                "--type",
                "sagemaker",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--region",
                "us-east-1",
                "--execution-role",
                "arn:aws:iam::123456789012:role/SageMakerRole",
                "--credentials",
                "aws-profile:ml-team",
                "--no-verify",
            ]
        )

    assert exc_info.value.code == 0
    sagemaker_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(sagemaker_spec, SagemakerStackSpec)
    assert sagemaker_spec.model_dump(mode="json") == {
        "artifact_store": "s3://bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "region": "us-east-1",
        "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
        "credentials": "aws-profile:ml-team",
        "verify": False,
    }


def test_stack_create_azureml_builds_spec() -> None:
    """AzureML stacks should build the shared AzureML spec cleanly."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-azure",
            stack_type="azureml",
            resources={
                "provider": "azure",
                "subscription_id": "00000000-0000-0000-0000-000000000123",
                "resource_group": "rg-demo",
                "workspace": "ws-demo",
                "region": "westeurope",
                "artifact_store": "az://container/kitaru",
                "container_registry": "demo.azurecr.io/team/image",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-azure",
                "--type",
                "azureml",
                "--artifact-store",
                "az://container/kitaru",
                "--container-registry",
                "demo.azurecr.io/team/image",
                "--subscription-id",
                "00000000-0000-0000-0000-000000000123",
                "--resource-group",
                "rg-demo",
                "--workspace",
                "ws-demo",
                "--region",
                "westeurope",
                "--credentials",
                "azure-access-token:token-123",
                "--no-verify",
            ]
        )

    assert exc_info.value.code == 0
    azureml_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(azureml_spec, AzureMLStackSpec)
    assert azureml_spec.model_dump(mode="json") == {
        "artifact_store": "az://container/kitaru",
        "container_registry": "demo.azurecr.io/team/image",
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "resource_group": "rg-demo",
        "workspace": "ws-demo",
        "region": "westeurope",
        "credentials": "azure-access-token:token-123",
        "verify": False,
    }


def test_stack_create_sagemaker_builds_spec_from_yaml_file(tmp_path: Path) -> None:
    """SageMaker stack creation should accept execution_role from YAML input."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-sagemaker
type: sagemaker
artifact_store: s3://bucket/kitaru
container_registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
region: us-east-1
execution_role: arn:aws:iam::123456789012:role/SageMakerRole
credentials: aws-profile:ml-team
verify: false
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="yaml-sagemaker",
            stack_type="sagemaker",
        )
        app(["stack", "create", "--file", str(stack_file)])

    assert exc_info.value.code == 0
    sagemaker_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(sagemaker_spec, SagemakerStackSpec)
    assert sagemaker_spec.model_dump(mode="json") == {
        "artifact_store": "s3://bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "region": "us-east-1",
        "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
        "credentials": "aws-profile:ml-team",
        "verify": False,
    }


def test_stack_create_azureml_builds_spec_from_yaml_and_cli_override(
    tmp_path: Path,
) -> None:
    """AzureML stack creation should support YAML input and CLI precedence."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-azure
type: azureml
artifact_store: az://container/kitaru
container_registry: demo.azurecr.io/team/image
subscription-id: 00000000-0000-0000-0000-000000000123
resource-group: rg-yaml
workspace: ws-yaml
region: westeurope
credentials: implicit
verify: true
activate: false
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="yaml-azure",
            stack_type="azureml",
            previous_active_stack=None,
        )
        app(
            [
                "stack",
                "create",
                "--file",
                str(stack_file),
                "--workspace",
                "ws-cli",
                "--no-verify",
            ]
        )

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.args == ("yaml-azure",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.AZUREML
    assert mock_create_stack.call_args.kwargs["activate"] is False
    azureml_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(azureml_spec, AzureMLStackSpec)
    assert azureml_spec.model_dump(mode="json") == {
        "artifact_store": "az://container/kitaru",
        "container_registry": "demo.azurecr.io/team/image",
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "resource_group": "rg-yaml",
        "workspace": "ws-cli",
        "region": "westeurope",
        "credentials": "implicit",
        "verify": False,
    }


def test_stack_create_vertex_passes_extra_and_async_overrides() -> None:
    """Advanced CLI stack-create flags should pass merged component overrides."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-vertex",
            stack_type="vertex",
        )
        app(
            [
                "stack",
                "create",
                "my-vertex",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
                "--extra",
                "orchestrator.pipeline_root=gs://bucket/root",
                "--extra",
                "container_registry.default_repository=my-team",
                "--async",
            ]
        )

    assert exc_info.value.code == 0
    overrides = mock_create_stack.call_args.kwargs["component_overrides"]
    assert isinstance(overrides, StackComponentConfigOverrides)
    assert overrides.model_dump() == {
        "orchestrator": {
            "pipeline_root": "gs://bucket/root",
            "synchronous": False,
        },
        "artifact_store": {},
        "container_registry": {"default_repository": "my-team"},
    }


def test_stack_create_extra_beats_async_default() -> None:
    """Explicit `--extra orchestrator.synchronous=...` should beat `--async`."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-vertex",
            stack_type="vertex",
        )
        app(
            [
                "stack",
                "create",
                "my-vertex",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
                "--extra",
                "orchestrator.synchronous=true",
                "--async",
            ]
        )

    assert exc_info.value.code == 0
    overrides = mock_create_stack.call_args.kwargs["component_overrides"]
    assert isinstance(overrides, StackComponentConfigOverrides)
    assert overrides.orchestrator == {"synchronous": True}


def test_stack_create_local_rejects_async_flag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--async` should only be valid for remote stack types."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--async"])

    assert exc_info.value.code == 1
    assert (
        "--async requires --type kubernetes, --type vertex, "
        "--type sagemaker, or --type azureml."
    ) in capsys.readouterr().err


def test_stack_create_merges_yaml_and_cli_component_overrides(tmp_path: Path) -> None:
    """YAML `extra:` config should merge with repeatable CLI `--extra` flags."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-vertex
type: vertex
artifact_store: gs://bucket/kitaru
container_registry: us-central1-docker.pkg.dev/demo/repo
region: us-central1
async: true
extra:
  orchestrator:
    pipeline_root: gs://bucket/root
  container_registry:
    default_repository: from-yaml
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="yaml-vertex",
            stack_type="vertex",
        )
        app(
            [
                "stack",
                "create",
                "-f",
                str(stack_file),
                "--extra",
                "orchestrator.custom_job_parameters.machine_type=n1-standard-4",
                "--extra",
                "container_registry.default_repository=from-cli",
            ]
        )

    assert exc_info.value.code == 0
    overrides = mock_create_stack.call_args.kwargs["component_overrides"]
    assert isinstance(overrides, StackComponentConfigOverrides)
    assert overrides.model_dump() == {
        "orchestrator": {
            "pipeline_root": "gs://bucket/root",
            "custom_job_parameters": {"machine_type": "n1-standard-4"},
            "synchronous": False,
        },
        "artifact_store": {},
        "container_registry": {"default_repository": "from-cli"},
    }


def test_stack_create_kubernetes_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Kubernetes stack creation should render provider/resource details."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-k8s",
            stack_type="kubernetes",
            resources={
                "provider": "aws",
                "cluster": "demo-cluster",
                "region": "us-east-1",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-k8s",
                "--type",
                "kubernetes",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--cluster",
                "demo-cluster",
                "--region",
                "us-east-1",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Created stack: my-k8s (kubernetes)" in output
    assert "Provider:" in output and "aws" in output
    assert "Cluster:" in output and "demo-cluster (us-east-1)" in output
    assert "Artifacts:" in output and "s3://bucket/kitaru" in output
    assert (
        "Registry:" in output
        and "123456789012.dkr.ecr.us-east-1.amazonaws.com" in output
    )
    assert "Active stack: default → my-k8s" in output


def test_stack_create_vertex_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Vertex stack creation should render GCP resource details without a cluster."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-vertex",
            stack_type="vertex",
            resources={
                "provider": "gcp",
                "region": "us-central1",
                "artifact_store": "gs://bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/demo/repo",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-vertex",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Created stack: my-vertex (vertex)" in output
    assert "Provider:" in output and "gcp" in output
    assert "Region:" in output and "us-central1" in output
    assert "Artifacts:" in output and "gs://bucket/kitaru" in output
    assert "Registry:" in output and "us-central1-docker.pkg.dev/demo/repo" in output
    assert "Cluster:" not in output
    assert "Active stack: default → my-vertex" in output


def test_stack_create_sagemaker_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SageMaker stack creation should render AWS resource details without a cluster."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-sagemaker",
            stack_type="sagemaker",
            resources={
                "provider": "aws",
                "region": "us-east-1",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-sagemaker",
                "--type",
                "sagemaker",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--region",
                "us-east-1",
                "--execution-role",
                "arn:aws:iam::123456789012:role/SageMakerRole",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Created stack: my-sagemaker (sagemaker)" in output
    assert "Provider:" in output and "aws" in output
    assert "Region:" in output and "us-east-1" in output
    assert "Artifacts:" in output and "s3://bucket/kitaru" in output
    assert (
        "Registry:" in output
        and "123456789012.dkr.ecr.us-east-1.amazonaws.com" in output
    )
    assert (
        "Execution role:" in output
        and "arn:aws:iam::123456789012:role/SageMakerRole" in output
    )
    assert "Cluster:" not in output
    assert "Active stack: default → my-sagemaker" in output


def test_stack_create_azureml_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AzureML stack creation should render Azure resource details cleanly."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-azure",
            stack_type="azureml",
            resources={
                "provider": "azure",
                "subscription_id": "00000000-0000-0000-0000-000000000123",
                "resource_group": "rg-demo",
                "workspace": "ws-demo",
                "region": "westeurope",
                "artifact_store": "az://container/kitaru",
                "container_registry": "demo.azurecr.io/team/image",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-azure",
                "--type",
                "azureml",
                "--artifact-store",
                "az://container/kitaru",
                "--container-registry",
                "demo.azurecr.io/team/image",
                "--subscription-id",
                "00000000-0000-0000-0000-000000000123",
                "--resource-group",
                "rg-demo",
                "--workspace",
                "ws-demo",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Created stack: my-azure (azureml)" in output
    assert "Provider:" in output and "azure" in output
    assert (
        "Subscription:" in output and "00000000-0000-0000-0000-000000000123" in output
    )
    assert "Resource group:" in output and "rg-demo" in output
    assert "Workspace:" in output and "ws-demo" in output
    assert "Region:" in output and "westeurope" in output
    assert "Artifacts:" in output and "az://container/kitaru" in output
    assert "Registry:" in output and "demo.azurecr.io/team/image" in output
    assert "Active stack: default → my-azure" in output


def test_stack_create_kubernetes_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Kubernetes stack creation JSON should include future-ready metadata."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-k8s",
            stack_type="kubernetes",
            components_created=(
                "my-k8s-orchestrator (orchestrator)",
                "my-k8s-artifacts (artifact_store)",
                "my-k8s-registry (container_registry)",
            ),
            service_connectors_created=("my-k8s-aws",),
            resources={
                "provider": "aws",
                "cluster": "demo-cluster",
                "region": "us-east-1",
                "namespace": "ml",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-k8s",
                "--type",
                "kubernetes",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--cluster",
                "demo-cluster",
                "--region",
                "us-east-1",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.create",
        "item": {
            "id": "stack-my-k8s-id",
            "name": "my-k8s",
            "is_active": True,
            "previous_active_stack": "default",
            "components_created": [
                "my-k8s-orchestrator (orchestrator)",
                "my-k8s-artifacts (artifact_store)",
                "my-k8s-registry (container_registry)",
            ],
            "stack_type": "kubernetes",
            "service_connectors_created": ["my-k8s-aws"],
            "resources": {
                "provider": "aws",
                "cluster": "demo-cluster",
                "region": "us-east-1",
                "namespace": "ml",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            },
        },
    }


def test_stack_create_vertex_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Vertex stack creation JSON should expose the new stack type cleanly."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-vertex",
            stack_type="vertex",
            components_created=(
                "my-vertex-orchestrator (orchestrator)",
                "my-vertex-artifacts (artifact_store)",
                "my-vertex-registry (container_registry)",
            ),
            service_connectors_created=("my-vertex-gcp",),
            resources={
                "provider": "gcp",
                "region": "us-central1",
                "artifact_store": "gs://bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/demo/repo",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-vertex",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.create",
        "item": {
            "id": "stack-my-vertex-id",
            "name": "my-vertex",
            "is_active": True,
            "previous_active_stack": "default",
            "components_created": [
                "my-vertex-orchestrator (orchestrator)",
                "my-vertex-artifacts (artifact_store)",
                "my-vertex-registry (container_registry)",
            ],
            "stack_type": "vertex",
            "service_connectors_created": ["my-vertex-gcp"],
            "resources": {
                "provider": "gcp",
                "region": "us-central1",
                "artifact_store": "gs://bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/demo/repo",
            },
        },
    }


def test_stack_create_sagemaker_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SageMaker stack creation JSON should expose the new stack type cleanly."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-sagemaker",
            stack_type="sagemaker",
            components_created=(
                "my-sagemaker-orchestrator (orchestrator)",
                "my-sagemaker-artifacts (artifact_store)",
                "my-sagemaker-registry (container_registry)",
            ),
            service_connectors_created=("my-sagemaker-aws",),
            resources={
                "provider": "aws",
                "region": "us-east-1",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-sagemaker",
                "--type",
                "sagemaker",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--region",
                "us-east-1",
                "--execution-role",
                "arn:aws:iam::123456789012:role/SageMakerRole",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.create",
        "item": {
            "id": "stack-my-sagemaker-id",
            "name": "my-sagemaker",
            "is_active": True,
            "previous_active_stack": "default",
            "components_created": [
                "my-sagemaker-orchestrator (orchestrator)",
                "my-sagemaker-artifacts (artifact_store)",
                "my-sagemaker-registry (container_registry)",
            ],
            "stack_type": "sagemaker",
            "service_connectors_created": ["my-sagemaker-aws"],
            "resources": {
                "provider": "aws",
                "region": "us-east-1",
                "artifact_store": "s3://bucket/kitaru",
                "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
            },
        },
    }


def test_stack_create_azureml_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AzureML stack creation JSON should expose the new stack type cleanly."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="my-azure",
            stack_type="azureml",
            components_created=(
                "my-azure-orchestrator (orchestrator)",
                "my-azure-artifacts (artifact_store)",
                "my-azure-registry (container_registry)",
            ),
            service_connectors_created=("my-azure-connector",),
            resources={
                "provider": "azure",
                "subscription_id": "00000000-0000-0000-0000-000000000123",
                "resource_group": "rg-demo",
                "workspace": "ws-demo",
                "region": "westeurope",
                "artifact_store": "az://container/kitaru",
                "container_registry": "demo.azurecr.io/team/image",
            },
        )
        app(
            [
                "stack",
                "create",
                "my-azure",
                "--type",
                "azureml",
                "--artifact-store",
                "az://container/kitaru",
                "--container-registry",
                "demo.azurecr.io/team/image",
                "--subscription-id",
                "00000000-0000-0000-0000-000000000123",
                "--resource-group",
                "rg-demo",
                "--workspace",
                "ws-demo",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.create",
        "item": {
            "id": "stack-my-azure-id",
            "name": "my-azure",
            "is_active": True,
            "previous_active_stack": "default",
            "components_created": [
                "my-azure-orchestrator (orchestrator)",
                "my-azure-artifacts (artifact_store)",
                "my-azure-registry (container_registry)",
            ],
            "stack_type": "azureml",
            "service_connectors_created": ["my-azure-connector"],
            "resources": {
                "provider": "azure",
                "subscription_id": "00000000-0000-0000-0000-000000000123",
                "resource_group": "rg-demo",
                "workspace": "ws-demo",
                "region": "westeurope",
                "artifact_store": "az://container/kitaru",
                "container_registry": "demo.azurecr.io/team/image",
            },
        },
    }


def test_stack_create_from_file_builds_local_stack(tmp_path: Path) -> None:
    """YAML-only local stack creation should use file inputs."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-local
type: local
activate: true
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(name="yaml-local")
        app(["stack", "create", "--file", str(stack_file)])

    assert exc_info.value.code == 0
    mock_create_stack.assert_called_once_with(
        "yaml-local",
        stack_type=StackType.LOCAL,
        activate=True,
        remote_spec=None,
    )


def test_stack_create_from_file_builds_kubernetes_stack(tmp_path: Path) -> None:
    """YAML-only Kubernetes creation should build the same structured spec as flags."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-k8s
type: kubernetes
artifact_store: s3://bucket/kitaru
container_registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
cluster: demo-cluster
region: us-east-1
namespace: ml
credentials: aws-profile:demo
verify: false
activate: false
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="yaml-k8s",
            stack_type="kubernetes",
            previous_active_stack=None,
        )
        app(["stack", "create", "-f", str(stack_file)])

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.args == ("yaml-k8s",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.KUBERNETES
    assert mock_create_stack.call_args.kwargs["activate"] is False
    kubernetes_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(kubernetes_spec, KubernetesStackSpec)
    assert kubernetes_spec.model_dump(mode="json") == {
        "provider": "aws",
        "artifact_store": "s3://bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "cluster": "demo-cluster",
        "region": "us-east-1",
        "namespace": "ml",
        "credentials": "aws-profile:demo",
        "verify": False,
    }


def test_stack_create_from_file_builds_vertex_stack(tmp_path: Path) -> None:
    """YAML-only Vertex creation should build the same structured spec as flags."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-vertex
type: vertex
artifact_store: gs://bucket/kitaru
container_registry: us-central1-docker.pkg.dev/demo/repo
region: us-central1
credentials: gcp-service-account:/tmp/key.json
verify: false
activate: false
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="yaml-vertex",
            stack_type="vertex",
            previous_active_stack=None,
        )
        app(["stack", "create", "-f", str(stack_file)])

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.args == ("yaml-vertex",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.VERTEX
    assert mock_create_stack.call_args.kwargs["activate"] is False
    vertex_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(vertex_spec, VertexStackSpec)
    assert vertex_spec.model_dump(mode="json") == {
        "artifact_store": "gs://bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/demo/repo",
        "region": "us-central1",
        "credentials": "gcp-service-account:/tmp/key.json",
        "verify": False,
    }


def test_stack_create_cli_overrides_file_values(tmp_path: Path) -> None:
    """Explicit CLI values should override YAML inputs while preserving the rest."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-k8s
type: kubernetes
artifact_store: s3://bucket/kitaru
container_registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
cluster: demo-cluster
region: us-east-1
namespace: yaml-ns
credentials: aws-profile:demo
verify: true
activate: true
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(
            name="cli-name",
            stack_type="kubernetes",
            previous_active_stack=None,
        )
        app(
            [
                "stack",
                "create",
                "cli-name",
                "--file",
                str(stack_file),
                "--region",
                "eu-west-1",
                "--namespace",
                "cli-ns",
                "--no-activate",
                "--no-verify",
            ]
        )

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.args == ("cli-name",)
    assert mock_create_stack.call_args.kwargs["activate"] is False
    kubernetes_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(kubernetes_spec, KubernetesStackSpec)
    assert kubernetes_spec.model_dump(mode="json") == {
        "provider": "aws",
        "artifact_store": "s3://bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "cluster": "demo-cluster",
        "region": "eu-west-1",
        "namespace": "cli-ns",
        "credentials": "aws-profile:demo",
        "verify": False,
    }


def test_stack_create_from_file_uses_yaml_name_when_positional_omitted(
    tmp_path: Path,
) -> None:
    """File mode should allow omitting the positional stack name."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-name
type: local
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_stack.return_value = _stack_create_result_stub(name="yaml-name")
        app(["stack", "create", "-f", str(stack_file)])

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.args == ("yaml-name",)


def test_stack_create_from_file_requires_name_somewhere(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The merged create inputs still require a non-empty final name."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
type: local
""".strip(),
    )

    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "-f", str(stack_file)])

    assert exc_info.value.code == 1
    assert "Stack name or ID cannot be empty." in capsys.readouterr().err


def test_stack_create_from_file_surfaces_invalid_yaml_json_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invalid YAML file contents should route through the structured error path."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: broken
type: [unterminated
""".strip(),
    )

    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "-f", str(stack_file), "--output", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "stack.create"
    assert payload["error"]["type"] == "ValueError"
    assert "Invalid YAML in stack config file" in payload["error"]["message"]


def test_stack_create_from_file_rejects_unknown_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Unknown YAML keys should fail fast with a clear schema error."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-name
type: local
unexpected: true
""".strip(),
    )

    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "-f", str(stack_file)])

    assert exc_info.value.code == 1
    assert "Unsupported stack config keys" in capsys.readouterr().err


def test_stack_create_rejects_malformed_extra_json_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Malformed `--extra` assignments should use the structured JSON error path."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "stack",
                "create",
                "my-vertex",
                "--type",
                "vertex",
                "--artifact-store",
                "gs://bucket/kitaru",
                "--container-registry",
                "us-central1-docker.pkg.dev/demo/repo",
                "--region",
                "us-central1",
                "--extra",
                "orchestrator",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "command": "stack.create",
        "error": {
            "message": (
                "Invalid --extra value 'orchestrator'. Use TARGET.FIELD=VALUE."
            ),
            "type": "ValueError",
        },
    }


def test_stack_create_from_file_rejects_non_string_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Top-level YAML keys must stay string-based for predictable schema validation."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-name
1: local
""".strip(),
    )

    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "-f", str(stack_file)])

    assert exc_info.value.code == 1
    assert "can only use string keys" in capsys.readouterr().err


def test_stack_create_kubernetes_surfaces_backend_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI should surface Kubernetes backend failures without mangling them."""
    with (
        patch(
            "kitaru.cli._create_stack_operation",
            side_effect=RuntimeError(
                "Created Kubernetes stack 'my-k8s' but failed to activate it."
            ),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "stack",
                "create",
                "my-k8s",
                "--type",
                "kubernetes",
                "--artifact-store",
                "s3://bucket/kitaru",
                "--container-registry",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "--cluster",
                "demo-cluster",
                "--region",
                "us-east-1",
            ]
        )

    assert exc_info.value.code == 1
    assert (
        "Created Kubernetes stack 'my-k8s' but failed to activate it."
        in capsys.readouterr().err
    )


def test_stack_delete_reports_deleted_components_and_new_active_stack(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru stack delete` should render the full forced recursive summary."""
    with (
        patch("kitaru.cli._delete_stack_operation") as mock_delete_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="dev",
            components_deleted=("dev (orchestrator)", "dev (artifact_store)"),
            new_active_stack="default",
            recursive=True,
        )
        app(["stack", "delete", "dev", "--recursive", "--force"])

    assert exc_info.value.code == 0
    mock_delete_stack.assert_called_once_with(
        "dev",
        recursive=True,
        force=True,
    )
    output = capsys.readouterr().out
    assert "Deleted stack: dev" in output
    assert "Deleted components: dev (orchestrator), dev (artifact_store)" in output
    assert "Active stack: default" in output


def test_stack_delete_simple_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru stack delete` should keep simple non-recursive output compact."""
    with (
        patch("kitaru.cli._delete_stack_operation") as mock_delete_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="dev",
            components_deleted=(),
            new_active_stack=None,
            recursive=False,
        )
        app(["stack", "delete", "dev"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Deleted stack: dev" in output
    assert "Deleted components:" not in output
    assert "Active stack:" not in output


def test_stack_delete_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru stack delete --output json` should emit structured delete details."""
    with (
        patch("kitaru.cli._delete_stack_operation") as mock_delete_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="dev",
            components_deleted=("dev (orchestrator)", "dev (artifact_store)"),
            new_active_stack="default",
            recursive=True,
        )
        app(["stack", "delete", "dev", "--recursive", "--force", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.delete",
        "item": {
            "deleted_stack": "dev",
            "components_deleted": [
                "dev (orchestrator)",
                "dev (artifact_store)",
            ],
            "new_active_stack": "default",
            "recursive": True,
        },
    }


def test_stack_delete_kubernetes_output_includes_container_registry(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Recursive Kubernetes delete output should list the registry clearly."""
    with (
        patch("kitaru.cli._delete_stack_operation") as mock_delete_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="my-k8s",
            components_deleted=(
                "my-k8s-orchestrator (orchestrator)",
                "my-k8s-artifacts (artifact_store)",
                "my-k8s-registry (container_registry)",
            ),
            new_active_stack="default",
            recursive=True,
        )
        app(["stack", "delete", "my-k8s", "--recursive", "--force"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Deleted stack: my-k8s" in output
    assert "my-k8s-registry (container_registry)" in output
    assert "service connector" not in output.lower()


def test_stack_delete_kubernetes_json_output_keeps_existing_shape(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Recursive Kubernetes delete JSON should only expand the component list."""
    with (
        patch("kitaru.cli._delete_stack_operation") as mock_delete_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="my-k8s",
            components_deleted=(
                "my-k8s-orchestrator (orchestrator)",
                "my-k8s-artifacts (artifact_store)",
                "my-k8s-registry (container_registry)",
            ),
            new_active_stack="default",
            recursive=True,
        )
        app(
            [
                "stack",
                "delete",
                "my-k8s",
                "--recursive",
                "--force",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "stack.delete",
        "item": {
            "deleted_stack": "my-k8s",
            "components_deleted": [
                "my-k8s-orchestrator (orchestrator)",
                "my-k8s-artifacts (artifact_store)",
                "my-k8s-registry (container_registry)",
            ],
            "new_active_stack": "default",
            "recursive": True,
        },
    }


def test_status_renders_compact_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru status` should render the compact status view."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        config_directory="/tmp/kitaru-config",
        local_server_status="not started",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru status" in output
    assert "Connection: remote Kitaru server" in output
    assert "Active stack: prod" in output
    assert "Config directory: /tmp/kitaru-config" in output
    assert "Project override" not in output
    assert "Environment" not in output


def test_status_renders_environment_section_with_masking(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Status should show active KITARU env vars and mask secret values."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        config_directory="/tmp/kitaru-config",
        local_server_status="not started",
        environment=[
            ActiveEnvironmentVariable(
                name="KITARU_SERVER_URL",
                value="https://example.com",
            ),
            ActiveEnvironmentVariable(
                name="KITARU_AUTH_TOKEN",
                value="token-12***",
            ),
        ],
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Environment" in output
    assert "KITARU_SERVER_URL: https://example.com" in output
    assert "KITARU_AUTH_TOKEN: token-12***" in output


def test_status_renders_log_store_mismatch_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Status should include a compact log-store mismatch row + warning block."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        config_directory="/tmp/kitaru-config",
        local_server_status="not started",
        log_store_status="datadog (preferred) ⚠ stack uses artifact-store",
        log_store_warning=(
            "Active ZenML stack uses: artifact-store\n"
            "The Kitaru log-store preference is not wired into stack selection yet."
        ),
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Log store: datadog (preferred) ⚠ stack uses artifact-store" in output
    assert "Active ZenML stack uses: artifact-store" in output


def test_info_renders_detailed_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should render the richer diagnostic view."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        repository_root="/work/repo",
        server_version="0.94.0",
        server_database="sqlite",
        server_deployment_type="oss",
        config_directory="/tmp/kitaru-config",
        local_server_status="not started",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru info" in output
    assert "Connection target: https://example.com" in output
    assert "Server version: 0.94.0" in output
    assert "Repository root: /work/repo" in output
    assert "Project override" not in output


def test_info_shows_project_override_when_set(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should show project override only when explicitly set."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        config_directory="/tmp/kitaru-config",
        project_override="staging-project",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Project override: staging-project" in output


def test_remote_login_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """Remote login JSON output should include `mode: remote`."""
    with (
        patch("kitaru.cli.login_to_server") as mock_login,
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="https://example.com",
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login", "https://example.com/", "--project", "demo", "--output", "json"])

    assert exc_info.value.code == 0
    mock_login.assert_called_once()
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "login",
        "item": {
            "mode": "remote",
            "url": "https://example.com",
            "project": "demo",
        },
    }


def test_local_login_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """Bare local login JSON output should include `mode: local`."""
    with (
        patch(
            "kitaru.cli.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:8383",
                action="started",
            ),
        ) as mock_start,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login", "--output", "json"])

    assert exc_info.value.code == 0
    mock_start.assert_called_once_with(port=None, timeout=60)
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "login",
        "item": {
            "mode": "local",
            "url": "http://127.0.0.1:8383",
        },
    }


def test_logout_json_output_includes_local_server_cleanup(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Logout JSON output should report whether a local daemon was stopped."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="https://example.com/")
    fake_credentials_store = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch(
            "kitaru.cli._get_connected_server_url", return_value="https://example.com"
        ),
        patch(
            "kitaru.cli.stop_registered_local_server",
            return_value=SimpleNamespace(
                stopped=True,
                url="http://127.0.0.1:8383",
            ),
        ),
        patch(
            "kitaru.cli.get_credentials_store",
            return_value=fake_credentials_store,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["logout", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "logout",
        "item": {
            "mode": "remote_server",
            "target": "https://example.com",
            "local_fallback_available": True,
            "local_server_stopped": True,
        },
    }


def test_stack_list_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru stack list --output json` should emit serialized stacks."""
    with (
        patch("kitaru.cli._list_stack_entries") as mock_list_stack_entries,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_stack_entries.return_value = [
            SimpleNamespace(
                stack=SimpleNamespace(
                    id="stack-local-id",
                    name="local",
                    is_active=False,
                ),
                is_managed=False,
            ),
            SimpleNamespace(
                stack=SimpleNamespace(
                    id="stack-prod-id",
                    name="prod",
                    is_active=True,
                ),
                is_managed=True,
            ),
        ]
        app(["stack", "list", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "stack.list"
    assert payload["count"] == 2
    assert payload["items"][1]["is_active"] is True
    assert payload["items"][0]["is_managed"] is False
    assert payload["items"][1]["is_managed"] is True


def test_model_list_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru model list --output json` should emit serialized aliases."""
    with (
        patch("kitaru.cli.list_model_aliases") as mock_list_models,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_models.return_value = [
            SimpleNamespace(
                alias="fast",
                model="openai/gpt-4o-mini",
                secret="openai-creds",
                is_default=True,
            )
        ]
        app(["model", "list", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "model.list"
    assert payload["items"][0]["alias"] == "fast"
    assert payload["items"][0]["is_default"] is True


def test_secrets_set_json_output_accepts_output_before_assignments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets set --output json` should still parse assignment tokens."""
    fake_client = Mock()
    fake_client.create_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=True,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
        secret_values={"OPENAI_API_KEY": "sk-123"},
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "secrets",
                "set",
                "openai-creds",
                "--output",
                "json",
                "--OPENAI_API_KEY=sk-123",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "secrets.set"
    assert payload["item"]["name"] == "openai-creds"
    assert payload["item"]["result"] == "created"


def test_status_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru status --output json` should emit the full snapshot payload."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        config_directory="/tmp/kitaru-config",
        local_server_status="not started",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "status"
    assert payload["item"]["connection"] == "remote Kitaru server"
    assert payload["item"]["active_stack"] == "prod"


def test_build_runtime_snapshot_handles_missing_local_store() -> None:
    """Status/info should degrade gracefully if local mode support is missing."""
    with (
        patch(
            "kitaru.inspection.GlobalConfiguration", return_value=_BrokenGlobalConfig()
        ),
        patch("kitaru.inspection.get_local_server", side_effect=ImportError("missing")),
        patch("kitaru.inspection.resolve_installed_version", return_value="1.2.3"),
    ):
        snapshot = _build_runtime_snapshot()

    assert snapshot.sdk_version == "1.2.3"
    assert snapshot.connection == "local mode (unavailable)"
    assert snapshot.connection_target == "unavailable"
    assert (
        snapshot.local_server_status
        == "unavailable (local runtime support not installed)"
    )
    assert snapshot.warning is not None
    assert "Local Kitaru runtime support is unavailable" in snapshot.warning


def test_build_runtime_snapshot_short_circuits_stale_local_server() -> None:
    """Status should avoid expensive retries for a stopped localhost server."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="http://127.0.0.1:8237")
    fake_gc.config_directory = "/tmp/kitaru-config"
    fake_local_server = SimpleNamespace(
        config=SimpleNamespace(
            provider=SimpleNamespace(value="daemon"),
            port=8237,
            ip_address="127.0.0.1",
        ),
        status=SimpleNamespace(
            url=None,
            status_message="service daemon is not running",
        ),
    )

    with (
        patch("kitaru.inspection.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.inspection.connected_to_local_server", return_value=False),
        patch("kitaru.inspection.get_local_server", return_value=fake_local_server),
        patch(
            "kitaru.inspection.Client",
            side_effect=AssertionError("Client should not be queried"),
        ),
    ):
        snapshot = _build_runtime_snapshot()

    assert snapshot.warning is not None
    assert "stopped local server" in snapshot.warning


def test_describe_local_server_handles_missing_local_backend() -> None:
    """Local server rendering should not crash when local server extras are missing."""
    with patch(
        "kitaru.inspection.get_local_server", side_effect=ImportError("missing")
    ):
        status = _describe_local_server()

    assert status == "unavailable (local runtime support not installed)"


# ---------------------------------------------------------------------------
# Clean command tests
# ---------------------------------------------------------------------------


class TestCleanHelp:
    """Tests for clean command help and registration."""

    def test_clean_appears_in_help(self, capsys: pytest.CaptureFixture[str]) -> None:
        """'clean' should appear in top-level --help."""
        with pytest.raises(SystemExit) as exc_info:
            app(["--help"])
        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        assert "clean" in output

    def test_clean_help_shows_subcommands(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """'kitaru clean --help' should list project/global/all."""
        with pytest.raises(SystemExit) as exc_info:
            app(["clean", "--help"])
        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        assert "project" in output
        assert "global" in output
        assert "all" in output


class TestCleanProject:
    """Tests for kitaru clean project."""

    def test_dry_run_no_project_errors(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Dry-run on clean project should error when no project found."""
        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=None,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "project", "--dry-run"])
        assert exc_info.value.code == 1
        output = capsys.readouterr().err
        assert "No Kitaru project found" in output

    def test_dry_run_shows_preview(
        self,
        capsys: pytest.CaptureFixture[str],
        tmp_path: Path,
    ) -> None:
        """Dry-run should show what would be deleted."""
        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=tmp_path,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "project", "--dry-run"])
        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        assert "Would delete" in output
        assert ".kitaru" in output

    def test_dry_run_json_output(
        self,
        capsys: pytest.CaptureFixture[str],
        tmp_path: Path,
    ) -> None:
        """Dry-run JSON should emit a {command, item} envelope."""
        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=tmp_path,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "project", "--dry-run", "-o", "json"])
        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        payload = json.loads(output)
        assert payload["command"] == "clean.project"
        assert payload["item"]["scope"] == "project"
        assert payload["item"]["dry_run"] is True


class TestCleanGlobal:
    """Tests for kitaru clean global."""

    def test_force_required_when_aliases_exist(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """Should error when model registry has aliases and --force is missing."""
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "kitaru.yaml").write_text("version: 1\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=None,
            ),
            patch(
                "kitaru._cleanup._resolve_config_root",
                return_value=config_root,
            ),
            patch(
                "kitaru._cleanup._read_alias_count",
                return_value=3,
            ),
            patch(
                "kitaru._cleanup._describe_local_server_for_cleanup",
                return_value=("not running", False),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "global", "--yes"])
        assert exc_info.value.code == 1
        output = capsys.readouterr().err
        assert "3 aliases" in output
        assert "--force" in output

    def test_dry_run_shows_backup_path(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """Dry-run should mention backup path if a DB exists."""
        config_root = tmp_path / "config"
        config_root.mkdir()
        db_dir = config_root / "local_stores" / "default_zen_store"
        db_dir.mkdir(parents=True)
        (db_dir / "zenml.db").write_text("fake db")
        (config_root / "kitaru.yaml").write_text("version: 1\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=None,
            ),
            patch(
                "kitaru._cleanup._resolve_config_root",
                return_value=config_root,
            ),
            patch(
                "kitaru._cleanup._read_alias_count",
                return_value=0,
            ),
            patch(
                "kitaru._cleanup._describe_local_server_for_cleanup",
                return_value=("not running", False),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "global", "--dry-run"])
        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        assert "Backup" in output or "backup" in output


class TestCleanAll:
    """Tests for kitaru clean all."""

    def test_all_skips_missing_project_silently(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """'clean all' should not error when no project exists."""
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "kitaru.yaml").write_text("version: 1\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=None,
            ),
            patch(
                "kitaru._cleanup._resolve_config_root",
                return_value=config_root,
            ),
            patch(
                "kitaru._cleanup._read_alias_count",
                return_value=0,
            ),
            patch(
                "kitaru._cleanup._describe_local_server_for_cleanup",
                return_value=("not running", False),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "all", "--dry-run"])
        assert exc_info.value.code == 0


class TestExecuteCleanupPlan:
    """Tests for the actual deletion path of execute_cleanup_plan()."""

    def test_project_cleanup_deletes_directory(self, tmp_path: Path) -> None:
        """execute_cleanup_plan should delete the project config directory."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        plan = CleanupPlan(
            scope=CleanScope.PROJECT,
            repo_root=str(tmp_path),
            project_config_path=str(project_dir),
        )

        result = execute_cleanup_plan(plan, yes=True, force=False)

        assert not result.aborted
        assert not result.dry_run
        assert str(project_dir) in result.deleted_paths
        assert not project_dir.exists()

    def test_global_cleanup_creates_backup_before_deleting(
        self, tmp_path: Path
    ) -> None:
        """Backup should exist before config directory is removed."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        config_root = tmp_path / "config"
        config_root.mkdir()
        db_dir = config_root / "local_stores" / "default_zen_store"
        db_dir.mkdir(parents=True)
        db_file = db_dir / "zenml.db"
        db_file.write_text("fake database content")

        backup_path = str(tmp_path / "config-backups" / "backup-test.db")

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root=str(config_root),
            backup_path=backup_path,
            model_registry_alias_count=0,
        )

        with patch("kitaru._cleanup._reset_global_config"):
            result = execute_cleanup_plan(plan, yes=True, force=False)

        assert not result.aborted
        assert Path(backup_path).exists()
        assert Path(backup_path).read_text() == "fake database content"
        assert not config_root.exists()
        assert str(config_root) in result.deleted_paths

    def test_reinit_failure_produces_warning(self, tmp_path: Path) -> None:
        """Failed re-initialization should add a warning."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        plan = CleanupPlan(
            scope=CleanScope.PROJECT,
            repo_root=str(tmp_path),
            project_config_path=str(project_dir),
            can_reinitialize_project=True,
        )

        with patch(
            "kitaru._cleanup._reinitialize_project",
            return_value=False,
        ):
            result = execute_cleanup_plan(
                plan,
                yes=False,
                force=False,
                prompt_confirm=lambda _: True,
                prompt_reinitialize=lambda _: True,
            )

        assert any("re-initialize" in w.lower() for w in result.warnings)

    def test_deletion_failure_produces_warning(self, tmp_path: Path) -> None:
        """OSError during deletion should produce a warning, not a crash."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        plan = CleanupPlan(
            scope=CleanScope.PROJECT,
            repo_root=str(tmp_path),
            project_config_path=str(tmp_path / "nonexistent" / ".kitaru"),
        )

        with patch(
            "kitaru._cleanup._delete_directory",
            side_effect=OSError("Permission denied"),
        ):
            result = execute_cleanup_plan(
                plan,
                yes=True,
                force=False,
            )

        assert any("permission denied" in w.lower() for w in result.warnings)


# ---------------------------------------------------------------------------
# Enhanced info tests
# ---------------------------------------------------------------------------


def test_info_shows_zenml_version(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should show ZenML version."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/config",
        zenml_version="0.72.0",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "ZenML version: 0.72.0" in output


def test_info_shows_config_provenance(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should show config provenance section."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
        kitaru_global_config_path="/tmp/config/kitaru.yaml",
        zenml_global_config_path="/tmp/config/config.yaml",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Config provenance" in output
    assert "kitaru.yaml" in output


def test_info_shows_connection_sources(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should show connection source breakdown."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/config",
        connection_sources={
            "server_url": "environment (KITARU_SERVER_URL)",
            "auth_token": "global config",
            "project": "repo-local config (.kitaru/)",
        },
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Connection source" in output
    assert "environment (KITARU_SERVER_URL)" in output


def test_info_shows_system_info(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should show system section."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
        python_version="3.12.4",
        system_info={"os": "macOS 15.1 (arm64)", "architecture": "arm64"},
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "System" in output
    assert "Python version: 3.12.4" in output


def test_info_all_includes_packages(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info --all` should show packages section."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
        python_version="3.12.4",
        system_info={"os": "Linux", "architecture": "x86_64"},
        environment_type="native",
        packages={"kitaru": "0.3.0", "zenml": "0.72.0", "pydantic": "2.10.3"},
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "--all"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Packages" in output
    assert "pydantic: 2.10.3" in output


def test_info_file_export_json(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """`kitaru info --file` should write JSON and report path."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
    )
    export_path = tmp_path / "debug.json"

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "--file", str(export_path)])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert str(export_path) in output
    assert export_path.exists()
    data = json.loads(export_path.read_text())
    assert data["sdk_version"] == "0.3.0"


def test_info_file_export_json_mode(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """`kitaru info --file -o json` should emit a JSON envelope about the file."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
    )
    export_path = tmp_path / "debug.json"

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "--file", str(export_path), "-o", "json"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["command"] == "info"
    assert payload["item"]["file"] == str(export_path)
    assert payload["item"]["format"] == "json"


def test_info_shows_environment_and_log_store(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info` should include env vars and log store."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/config",
        log_store_status="datadog (preferred)",
        environment=[
            ActiveEnvironmentVariable(
                name="KITARU_SERVER_URL",
                value="https://example.com",
            ),
        ],
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "KITARU_SERVER_URL" in output
    assert "Log store: datadog" in output


class TestCLIAnalytics:
    """Tests that CLI commands emit the expected analytics events."""

    def test_init_emits_project_initialized_event(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """``kitaru init`` should emit PROJECT_INITIALIZED after success."""
        target = tmp_path / "analytics_init"
        target.mkdir()
        with (
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["init", str(target)])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.PROJECT_INITIALIZED,
            {"used_cwd": False},
        )

    def test_login_local_emits_login_completed_event(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Bare ``kitaru login`` should emit LOGIN_COMPLETED with local mode."""
        with (
            patch(
                "kitaru.cli.start_or_connect_local_server",
                return_value=SimpleNamespace(
                    url="http://127.0.0.1:8383",
                    action="started",
                ),
            ),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["login"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.LOGIN_COMPLETED,
            {"mode": "local", "action": "started"},
        )

    def test_login_remote_emits_login_completed_event(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """``kitaru login <server>`` should emit LOGIN_COMPLETED with remote mode."""
        with (
            patch("kitaru.cli.login_to_server"),
            patch(
                "kitaru.cli._get_connected_server_url",
                return_value="https://example.com",
            ),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["login", "https://example.com/", "--api-key", "secret-key"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.LOGIN_COMPLETED,
            {"mode": "remote", "project_provided": False},
        )

    def test_secrets_set_emits_secret_upserted_event(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """``kitaru secrets set`` should emit SECRET_UPSERTED after success."""
        fake_client = Mock()
        fake_client.create_secret.return_value = SimpleNamespace(
            name="openai-creds",
            id="secret-id",
        )

        with (
            patch("kitaru.cli.Client", return_value=fake_client),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "secrets",
                    "set",
                    "openai-creds",
                    "--OPENAI_API_KEY=sk-123",
                ]
            )

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.SECRET_UPSERTED,
            {
                "operation": "created",
                "key_count": 1,
            },
        )
