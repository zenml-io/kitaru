"""Tests for the kitaru CLI."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from zenml.exceptions import EntityExistsError
from zenml.zen_stores.rest_zen_store import RestZenStore

from kitaru._cli._executions import _execution_statistics_table
from kitaru._client._models import AuthAPIKey, AuthAPIKeyWithValue, AuthServiceAccount
from kitaru._client._statistics import (
    LLM_EXECUTION_STATISTICS_METRIC_SHORTCUTS_DISPLAY,
    normalize_execution_statistics_metrics,
)
from kitaru.analytics import AnalyticsEvent
from kitaru.cli import (
    ActiveConfigSelectionProvenance,
    RuntimeSnapshot,
    _build_runtime_snapshot,
    _describe_local_server,
    _format_table_timestamp,
    _logout_current_connection,
    _parse_secret_assignments,
    app,
)
from kitaru.client import (
    ExecutionStatistics,
    ExecutionStatisticsGroup,
    ExecutionStatus,
    LogEntry,
)
from kitaru.config import (
    KITARU_MODEL_REGISTRY_ENV,
    ActiveEnvironmentVariable,
    AzureMLStackSpec,
    ImageSettings,
    KitaruConfig,
    KubernetesStackSpec,
    ModelAliasConfig,
    ModelRegistryConfig,
    SagemakerStackSpec,
    StackComponentConfigOverrides,
    StackType,
    VertexStackSpec,
)
from kitaru.errors import (
    KitaruDeploymentInputValuesError,
    KitaruStackNotRemoteExecutableUsageError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.replay import ReplayPlanDocument, ReplayResultRow, ReplaySubmission


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


def _deployment_stub(
    *,
    flow: str = "demo_flow",
    version: int = 1,
    tags: dict[str, bool] | None = None,
    deployment_id: str | None = None,
) -> SimpleNamespace:
    """Build a lightweight deployment-shaped object for CLI tests."""
    return SimpleNamespace(
        deployment_id=deployment_id or f"dep-{flow}-{version}",
        flow=flow,
        version=version,
        tags=tags or {"default": True},
        commit_sha="abc123",
        commit_dirty=False,
        image_digest=None,
        created_at=datetime(2026, 4, 21, 10, 0, 0),
        schema={"type": "object"},
        stack="local",
    )


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
    llm_usage_summary: dict[str, Any] | None = None,
) -> SimpleNamespace:
    """Build a lightweight execution-shaped object for CLI tests."""
    return SimpleNamespace(
        exec_id=exec_id,
        flow_id=f"flow-{flow_name}",
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
        llm_usage_summary=llm_usage_summary,
        llm_usage_records=[],
    )


def _replay_submission_stub(
    *,
    at: str = "write_summary",
    wait: bool = True,
    tag: str | None = None,
    results: list[ReplayResultRow] | None = None,
    compare_url: str | None = None,
) -> ReplaySubmission:
    """Build a lightweight ReplaySubmission for CLI tests."""
    return ReplaySubmission.create(
        tag=tag,
        at=at,
        wait=wait,
        plan=ReplayPlanDocument(),
        results=results
        or [
            ReplayResultRow(
                original_exec_ref="kr-111",
                original_exec_id="kr-111",
                replay_exec_id="kr-222",
                status="submitted",
                compare_url="http://localhost:8237/compare?executions=kr-111,kr-222",
            )
        ],
        compare_url=compare_url,
        submission_id="rs-test",
    )


def test_cli_command_modules_use_dependency_seam_not_legacy_facade() -> None:
    """Command modules should not reach through the legacy ``kitaru.cli`` facade."""
    command_modules = sorted(Path("src/kitaru/_cli").glob("_*.py"))
    offenders = [
        path.name
        for path in command_modules
        if path.name != "_helpers.py" and "_facade_module" in path.read_text()
    ]

    assert offenders == []


def _project_stub(
    *,
    name: str = "prod",
    project_id: str | None = None,
    display_name: str | None = None,
    description: str | None = None,
    is_active: bool = False,
) -> SimpleNamespace:
    """Build a lightweight project object for CLI tests."""
    return SimpleNamespace(
        id=project_id or f"project-{name}-id",
        name=name,
        display_name=display_name,
        description=description,
        is_active=is_active,
    )


def _project_create_result_stub(
    *,
    name: str = "staging",
    activated: bool = True,
    is_active: bool | None = None,
    previous_active_project: str | None = "prod",
) -> SimpleNamespace:
    """Build a lightweight project-create result object for CLI tests."""
    return SimpleNamespace(
        project=_project_stub(
            name=name,
            is_active=activated if is_active is None else is_active,
        ),
        previous_active_project=previous_active_project,
        activated=activated,
    )


def _project_delete_result_stub(*, name: str = "staging") -> SimpleNamespace:
    """Build a lightweight project-delete result object for CLI tests."""
    return SimpleNamespace(deleted_project=_project_stub(name=name))


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
    default_components = (f"{name} (orchestrator)", f"{name} (artifact_store)")
    if stack_type == "local":
        default_components = (*default_components, f"{name} (sandbox)")

    return SimpleNamespace(
        stack=SimpleNamespace(id=f"stack-{name}-id", name=name, is_active=is_active),
        previous_active_stack=previous_active_stack,
        components_created=components_created or default_components,
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
            SimpleNamespace(
                role="sandbox",
                name=f"{name}-sandbox",
                backend="local",
                details=(),
                purpose=None,
            ),
        ],
    )


def _auth_service_account_stub(
    *,
    service_account_id: str = "sa-123",
    name: str = "ci-runner",
    full_name: str = "CI Runner",
    description: str = "CI automation",
    active: bool = True,
) -> AuthServiceAccount:
    """Build a lightweight auth service-account DTO for CLI tests."""
    return AuthServiceAccount(
        service_account_id=service_account_id,
        name=name,
        full_name=full_name,
        description=description,
        active=active,
        created_at=datetime(2026, 4, 24, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 4, 24, 8, 5, tzinfo=UTC),
        avatar_url=None,
    )


def _auth_api_key_stub(
    *,
    api_key_id: str = "key-123",
    name: str = "default",
    service_account_id: str = "sa-123",
    service_account_name: str = "ci-runner",
    description: str = "Default CI key",
    active: bool = True,
) -> AuthAPIKey:
    """Build a lightweight auth API-key DTO for CLI tests."""
    return AuthAPIKey(
        api_key_id=api_key_id,
        name=name,
        service_account_id=service_account_id,
        service_account_name=service_account_name,
        description=description,
        active=active,
        created_at=datetime(2026, 4, 24, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 4, 24, 8, 5, tzinfo=UTC),
        last_login=None,
        last_rotated=datetime(2026, 4, 24, 8, 10, tzinfo=UTC),
        retain_period_minutes=0,
    )


def _auth_management_client_stub(
    *,
    service_accounts: Mock | None = None,
    api_keys: Mock | None = None,
) -> SimpleNamespace:
    """Build a KitaruClient.for_auth_management() test double."""
    return SimpleNamespace(
        auth=SimpleNamespace(
            service_accounts=service_accounts or Mock(),
            api_keys=api_keys or Mock(),
        )
    )


def _write_stack_create_file(tmp_path: Path, content: str) -> Path:
    """Write a temporary stack-create YAML file for CLI tests."""
    path = tmp_path / "stack.yaml"
    path.write_text(content)
    return path


def test_format_table_timestamp_compact_cases() -> None:
    """List/table timestamps should be compact, stable, and forgiving."""
    assert _format_table_timestamp(None) == "-"
    assert _format_table_timestamp("") == "-"
    assert _format_table_timestamp("   ") == "-"
    assert _format_table_timestamp(datetime(2026, 3, 7, 10, 0, 5)) == (
        "2026-03-07 10:00:05"
    )
    assert _format_table_timestamp(datetime(2026, 3, 7, 10, 0, 5, tzinfo=UTC)) == (
        "2026-03-07 10:00:05"
    )
    assert _format_table_timestamp("2026-04-13T14:05:30Z") == ("2026-04-13 14:05:30")
    assert _format_table_timestamp("not-a-timestamp") == "not-a-timestamp"


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
        "clean",
        "log-store",
        "stack",
        "secrets",
        "model",
        "executions",
        "build",
        "deploy",
        "invoke",
        "flow",
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


def test_flow_help_lists_deployment_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru flow --help` should show the deployment management surface."""
    with pytest.raises(SystemExit) as exc_info:
        app(["flow", "--help"])
    assert exc_info.value.code == 0
    output = capsys.readouterr().out.lower()
    for command in ("list", "show", "deployments", "tag", "untag"):
        assert command in output


def test_flow_deployments_help_lists_supported_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru flow deployments --help` should show version commands."""
    with pytest.raises(SystemExit) as exc_info:
        app(["flow", "deployments", "--help"])
    assert exc_info.value.code == 0
    output = capsys.readouterr().out.lower()
    for command in ("list", "show", "curl", "logs", "delete"):
        assert command in output


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
        "resume",
        "retry",
        "cancel",
        "statistics",
    ):
        assert command in output


def test_auth_help_lists_supported_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru auth --help` should show token and auth-management groups."""
    with pytest.raises(SystemExit) as exc_info:
        app(["auth", "--help"])
    assert exc_info.value.code == 0
    output = capsys.readouterr().out.lower()
    for command in ("token", "service-accounts", "api-keys"):
        assert command in output


def test_auth_token_text_prints_token_only(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Text mode should print only the access token plus a newline."""
    fake_store = Mock(spec=RestZenStore)
    fake_store.get_or_generate_api_token.return_value = "server-access-token"
    fake_client = SimpleNamespace(zen_store=fake_store)

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        patch("kitaru._cli._auth.resolve_connection_config") as resolve_mock,
        patch("kitaru._cli._auth.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "token"])

    assert exc_info.value.code == 0
    resolve_mock.assert_called_once_with(
        validate_for_use=True,
        require_project=False,
    )
    assert capsys.readouterr().out == "server-access-token\n"
    track_mock.assert_called_once_with(
        AnalyticsEvent.AUTH_TOKEN_PRINTED,
        {"command": "auth.token"},
    )


def test_auth_token_json_includes_token(capsys: pytest.CaptureFixture[str]) -> None:
    """JSON mode should wrap the access token in the standard command envelope."""
    fake_store = Mock(spec=RestZenStore)
    fake_store.get_or_generate_api_token.return_value = "server-access-token"
    fake_client = SimpleNamespace(zen_store=fake_store)

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        patch("kitaru._cli._auth.resolve_connection_config"),
        patch("kitaru._cli._auth.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "token", "-o", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "auth.token",
        "item": {"token": "server-access-token"},
    }


def test_auth_token_uses_public_env_active_connection(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public auth env vars should be enough to mint a server access token."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://env-kitaru.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "org-level-token")
    monkeypatch.setenv("KITARU_PROJECT", "demo-project")
    fake_store = Mock(spec=RestZenStore)
    fake_store.get_or_generate_api_token.return_value = "server-access-token"
    fake_client = SimpleNamespace(zen_store=fake_store)

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        patch("kitaru._cli._auth.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "token"])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == "server-access-token\n"
    fake_store.get_or_generate_api_token.assert_called_once_with()


def test_auth_token_token_only_env_reports_clean_cli_error(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Token-only env should fail as a Kitaru CLI error, not a traceback."""
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "dummy-token")
    monkeypatch.delenv("KITARU_SERVER_URL", raising=False)

    with (
        patch(
            "kitaru.config._read_global_connection_config", return_value=KitaruConfig()
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "token"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert captured.out == ""
    assert (
        "Error: KITARU_AUTH_TOKEN is set but no Kitaru server URL is available"
        in captured.err
    )
    assert "set KITARU_SERVER_URL or run `kitaru login`" in captured.err
    assert "Traceback" not in captured.err


def test_auth_service_accounts_create_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Service-account create should call the SDK auth namespace and render text."""
    service_accounts = Mock()
    service_accounts.create.return_value = _auth_service_account_stub()
    fake_client = _auth_management_client_stub(service_accounts=service_accounts)

    with (
        patch(
            "kitaru.cli.KitaruClient.for_auth_management",
            return_value=fake_client,
        ) as client_factory,
        patch("kitaru._cli._auth.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "auth",
                "service-accounts",
                "create",
                "ci-runner",
                "--full-name",
                "CI Runner",
                "--description",
                "CI automation",
            ]
        )

    assert exc_info.value.code == 0
    client_factory.assert_called_once_with()
    service_accounts.create.assert_called_once_with(
        "ci-runner",
        full_name="CI Runner",
        description="CI automation",
    )
    output = capsys.readouterr().out
    assert "Created service account: ci-runner" in output
    assert "Service account" in output
    assert "Name: ci-runner" in output
    track_mock.assert_called_once_with(
        AnalyticsEvent.AUTH_SERVICE_ACCOUNT_CREATED,
        {"command": "auth.service-accounts.create", "has_description": True},
    )


def test_auth_service_accounts_list_json_contract(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Service-account list JSON should use the standard list envelope."""
    service_accounts = Mock()
    service_accounts.list.return_value = [_auth_service_account_stub()]
    fake_client = _auth_management_client_stub(service_accounts=service_accounts)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "auth",
                "service-accounts",
                "list",
                "--page",
                "2",
                "--size",
                "5",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    service_accounts.list.assert_called_once_with(
        active=None,
        name=None,
        page=2,
        size=5,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "auth.service-accounts.list"
    assert payload["count"] == 1
    assert payload["items"] == [
        {
            "service_account_id": "sa-123",
            "name": "ci-runner",
            "full_name": "CI Runner",
            "description": "CI automation",
            "active": True,
            "created_at": "2026-04-24T08:00:00+00:00",
            "updated_at": "2026-04-24T08:05:00+00:00",
            "avatar_url": None,
        }
    ]


def test_auth_api_keys_create_json_includes_one_time_key(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """API-key create JSON may include the raw key and activation status."""
    api_keys = Mock()
    api_keys.create.return_value = AuthAPIKeyWithValue(
        api_key=_auth_api_key_stub(),
        key="raw-api-key",
        local_key_activation_requested=True,
        local_key_activation_succeeded=False,
        local_key_activation_error=(
            "API key was created, but Kitaru could not set it as the active "
            "local credential: local store rejected [redacted]"
        ),
        local_key_rollback_attempted=False,
        local_key_rollback_succeeded=None,
        local_key_rollback_reason="No previous persisted local API key was available.",
    )
    fake_client = _auth_management_client_stub(api_keys=api_keys)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        patch("kitaru._cli._auth.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "auth",
                "api-keys",
                "create",
                "ci-runner",
                "default",
                "--description",
                "Default CI key",
                "--set-key",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    api_keys.create.assert_called_once_with(
        "ci-runner",
        "default",
        description="Default CI key",
        set_key=True,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "auth.api-keys.create"
    assert payload["item"]["key"] == "raw-api-key"
    assert payload["item"]["api_key_id"] == "key-123"
    assert payload["item"]["local_key_activation_requested"] is True
    assert payload["item"]["local_key_activation_succeeded"] is False
    assert (
        "local store rejected [redacted]"
        in (payload["item"]["local_key_activation_error"])
    )
    assert payload["item"]["local_key_rollback_attempted"] is False
    assert payload["item"]["local_key_rollback_succeeded"] is None
    assert payload["item"]["local_key_rollback_error"] is None
    assert (
        payload["item"]["local_key_rollback_reason"]
        == "No previous persisted local API key was available."
    )
    assert json.dumps(payload).count("raw-api-key") == 1


def test_auth_api_keys_list_and_show_json_do_not_leak_raw_key(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """API-key list/show JSON must contain metadata only, never `key`."""
    api_keys = Mock()
    api_keys.list.return_value = [_auth_api_key_stub()]
    api_keys.get.return_value = _auth_api_key_stub()
    fake_client = _auth_management_client_stub(api_keys=api_keys)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "api-keys", "list", "ci-runner", "-o", "json"])

    assert exc_info.value.code == 0
    list_payload = json.loads(capsys.readouterr().out)
    assert list_payload["command"] == "auth.api-keys.list"
    assert list_payload["count"] == 1
    assert "key" not in list_payload["items"][0]
    assert "raw-api-key" not in json.dumps(list_payload)
    api_keys.list.assert_called_once_with(
        "ci-runner",
        active=None,
        name=None,
        page=1,
        size=20,
    )

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "api-keys", "show", "ci-runner", "default", "-o", "json"])

    assert exc_info.value.code == 0
    show_payload = json.loads(capsys.readouterr().out)
    assert show_payload["command"] == "auth.api-keys.show"
    assert "key" not in show_payload["item"]
    assert "raw-api-key" not in json.dumps(show_payload)
    api_keys.get.assert_called_once_with("ci-runner", "default")


def test_auth_api_keys_rotate_text_includes_warning_and_new_key(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """API-key rotate text should show the key and activation failure warning."""
    api_keys = Mock()
    api_keys.rotate.return_value = AuthAPIKeyWithValue(
        api_key=_auth_api_key_stub(),
        key="rotated-raw-api-key",
        local_key_activation_requested=True,
        local_key_activation_succeeded=False,
        local_key_activation_error=(
            "API key was rotated, but Kitaru could not set it as the active "
            "local credential: local store rejected [redacted]. Kitaru also "
            "tried to restore the previous local credential, but that rollback "
            "failed. The server-side API key was still rotated; local "
            "credentials may need manual repair."
        ),
        local_key_rollback_attempted=True,
        local_key_rollback_succeeded=False,
        local_key_rollback_error="could not restore [redacted] locally",
    )
    fake_client = _auth_management_client_stub(api_keys=api_keys)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        patch("kitaru._cli._auth.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "auth",
                "api-keys",
                "rotate",
                "ci-runner",
                "default",
                "--retain-minutes",
                "10",
                "--set-key",
            ]
        )

    assert exc_info.value.code == 0
    api_keys.rotate.assert_called_once_with(
        "ci-runner",
        "default",
        retain_period_minutes=10,
        set_key=True,
    )
    output = capsys.readouterr().out
    assert "Rotated API key: default" in output
    assert "Key: rotated-raw-api-key" in output
    assert "Local activation: failed" in output
    assert "Credential rollback: failed" in output
    assert "Store this key now; it cannot be retrieved later." in output
    assert "local store rejected [redacted]" in output
    assert "manual repair" in output
    assert "kitaru login <server-url> --api-key <key>" in output
    assert output.count("rotated-raw-api-key") == 1


def test_auth_delete_requires_yes_in_non_interactive_mode(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Delete commands should not run in CI-like contexts unless --yes is set."""
    service_accounts = Mock()
    fake_client = _auth_management_client_stub(service_accounts=service_accounts)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "service-accounts", "delete", "ci-runner"])

    assert exc_info.value.code == 1
    service_accounts.delete.assert_not_called()
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "requires --yes" in captured.err


def test_auth_delete_with_yes_runs_and_returns_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--yes should skip confirmation and emit a deletion envelope in JSON mode."""
    service_accounts = Mock()
    fake_client = _auth_management_client_stub(service_accounts=service_accounts)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        patch("kitaru._cli._auth.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["auth", "service-accounts", "delete", "ci-runner", "--yes", "-o", "json"])

    assert exc_info.value.code == 0
    service_accounts.delete.assert_called_once_with("ci-runner")
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "auth.service-accounts.delete",
        "item": {"name_or_id": "ci-runner", "deleted": True},
    }


def test_auth_api_key_delete_with_yes_runs_and_returns_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """API-key delete should also honor --yes and use the API-key envelope."""
    api_keys = Mock()
    fake_client = _auth_management_client_stub(api_keys=api_keys)

    with (
        patch("kitaru.cli.KitaruClient.for_auth_management", return_value=fake_client),
        patch("kitaru._cli._auth.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "auth",
                "api-keys",
                "delete",
                "ci-runner",
                "default",
                "--yes",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    api_keys.delete.assert_called_once_with("ci-runner", "default")
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "auth.api-keys.delete",
        "item": {
            "service_account": "ci-runner",
            "name_or_id": "default",
            "deleted": True,
        },
    }


def test_build_requires_initialized_project_for_file_targets(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru build` should fail fast outside an initialized project."""
    flow_file = tmp_path / "demo.py"
    flow_file.write_text("demo_flow = object()\n")

    with pytest.raises(SystemExit) as exc_info:
        app(["build", f"{flow_file}:demo_flow", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "initialized Kitaru project" in payload["error"]["message"]
    assert "kitaru init" in payload["error"]["message"]


def test_deploy_accepts_legacy_zen_project_marker(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy `.zen/` markers should satisfy the CLI deploy preflight."""
    (tmp_path / ".zen").mkdir()
    flow_file = tmp_path / "demo.py"
    flow_file.write_text("demo_flow = object()\n")
    fake_flow = Mock()
    fake_flow.deploy.return_value = _deployment_stub()
    monkeypatch.chdir(tmp_path)

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["deploy", f"{flow_file}:demo_flow", "-o", "json"])

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(tags={"default": True})
    assert json.loads(capsys.readouterr().out)["command"] == "deploy"


def test_build_missing_file_still_reports_loader_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preflight should not mask the clearer missing-file loader error."""
    missing = tmp_path / "missing.py"

    with pytest.raises(SystemExit) as exc_info:
        app(["build", f"{missing}:demo_flow", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "Flow module path does not exist" in payload["error"]["message"]


def test_build_json_output_creates_deployment_from_target(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru build` should deploy a version without routing tags."""
    deployment = _deployment_stub(flow="demo_flow", version=1, tags={})
    fake_flow = Mock()
    fake_flow.deploy.return_value = deployment

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        patch("kitaru._cli._flows.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "build",
                "demo.py:demo_flow",
                "--input",
                '{"topic":"AI"}',
                "--image",
                "python:3.12-slim",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(
        tags={},
        topic="AI",
        image=ImageSettings(base_image="python:3.12-slim"),
        publish_default_on_first_deploy=False,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "build"
    assert payload["item"]["flow"] == "demo_flow"
    track_mock.assert_called_once_with(
        AnalyticsEvent.DEPLOYMENT_BUILT,
        {"command": "build", "has_input": True},
    )


def test_deploy_default_tag_passes_exclusive_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru deploy` should attach the reserved default tag by default."""
    deployment = _deployment_stub(flow="demo_flow", version=1)
    fake_flow = Mock()
    fake_flow.deploy.return_value = deployment

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        patch("kitaru._cli._flows.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["deploy", "demo.py:demo_flow", "-o", "json"])

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(tags={"default": True})
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["tags"] == {"default": True}
    track_mock.assert_called_once_with(
        AnalyticsEvent.DEPLOYMENT_DEPLOYED,
        {
            "command": "deploy",
            "has_input": False,
            "selector": "default",
            "exclusive": True,
        },
    )


def test_deploy_help_mentions_single_tag_follow_up(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deploy help should explain single-tag routing and follow-up tagging."""
    with pytest.raises(SystemExit) as exc_info:
        app(["deploy", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "one routing tag" in output
    assert "kitaru flow tag" in output


@pytest.mark.parametrize(
    ("command", "argv"),
    [
        ("build", ["build", "demo.py:demo_flow", "-o", "json"]),
        ("deploy", ["deploy", "demo.py:demo_flow", "-o", "json"]),
    ],
)
def test_build_and_deploy_stack_error_includes_stack_remedy(
    command: str,
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Build/deploy errors should point users to `--stack` and `kitaru stack use`."""
    fake_flow = Mock()
    fake_flow.deploy.side_effect = KitaruStackNotRemoteExecutableUsageError(
        "Flow 'demo_flow' cannot be deployed with stack 'local' because that "
        "stack is not one the Kitaru server can execute remotely."
    )

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(argv)

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == command
    message = payload["error"]["message"]
    assert "--stack <stack>" in message
    assert "kitaru stack use <stack>" in message


@pytest.mark.parametrize(
    ("command", "argv"),
    [
        ("build", ["build", "demo.py:demo_flow", "-o", "json"]),
        ("deploy", ["deploy", "demo.py:demo_flow", "-o", "json"]),
    ],
)
def test_build_and_deploy_input_error_includes_input_remedy(
    command: str,
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Build/deploy input-shape errors should point users to `--input`."""
    fake_flow = Mock()
    fake_flow.deploy.side_effect = KitaruDeploymentInputValuesError(
        "Unable to create this deployment because Kitaru needs concrete input "
        "values to prepare the saved deployment snapshot. Pass representative "
        "input values when calling flow.deploy(...), then override them later "
        "when invoking it."
    )

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(argv)

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == command
    message = payload["error"]["message"]
    assert "--input" in message
    assert "@inputs.json" in message


def test_build_input_file_parses_json_object(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--input @file` should read deployment-time inputs from JSON."""
    input_file = tmp_path / "inputs.json"
    input_file.write_text('{"topic":"cats"}')
    fake_flow = Mock()
    fake_flow.deploy.return_value = _deployment_stub(tags={})

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["build", "demo.py:demo_flow", "--input", f"@{input_file}", "-o", "json"])

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(
        tags={},
        topic="cats",
        publish_default_on_first_deploy=False,
    )
    assert json.loads(capsys.readouterr().out)["command"] == "build"


def test_deploy_image_json_object_is_normalized_and_forwarded(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--image` should accept structured JSON settings and normalize them once."""
    fake_flow = Mock()
    fake_flow.deploy.return_value = _deployment_stub()

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "deploy",
                "demo.py:demo_flow",
                "--image",
                '{"requirements":["kitaru[openai]"],"secret_environment_from":["openai-creds"]}',
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(
        tags={"default": True},
        image=ImageSettings(
            requirements=["kitaru[openai]"],
            secret_environment_from=["openai-creds"],
        ),
    )
    assert json.loads(capsys.readouterr().out)["command"] == "deploy"


def test_build_image_file_parses_json_object(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--image @file` should read a structured image payload from JSON."""
    image_file = tmp_path / "image.json"
    image_file.write_text('{"requirements":["numpy"],"base_image":"python:3.12-slim"}')
    fake_flow = Mock()
    fake_flow.deploy.return_value = _deployment_stub(tags={})

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "build",
                "demo.py:demo_flow",
                "--image",
                f"@{image_file}",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(
        tags={},
        image=ImageSettings(
            base_image="python:3.12-slim",
            requirements=["numpy"],
        ),
        publish_default_on_first_deploy=False,
    )
    assert json.loads(capsys.readouterr().out)["command"] == "build"


@pytest.mark.parametrize("inline_value", ["null", "true", "123"])
def test_build_accepts_inline_json_scalar_spelling_as_base_image(
    inline_value: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Inline scalar-looking values should be treated as base-image shorthand."""
    fake_flow = Mock()
    fake_flow.deploy.return_value = _deployment_stub(tags={})

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["build", "demo.py:demo_flow", "--image", inline_value, "-o", "json"])

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(
        tags={},
        image=ImageSettings(base_image=inline_value),
        publish_default_on_first_deploy=False,
    )
    assert json.loads(capsys.readouterr().out)["command"] == "build"


def test_build_rejects_structured_non_object_image_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--image` should reject JSON arrays even though object payloads are allowed."""
    with (
        patch("kitaru._cli._flows._load_deployable_flow_target") as mock_loader,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["build", "demo.py:demo_flow", "--image", "[]", "-o", "json"])

    assert exc_info.value.code == 1
    mock_loader.assert_not_called()
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "base image string or a JSON object" in payload["error"]["message"]


def test_build_image_file_accepts_plain_base_image_string(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--image @file` should treat plain text as a base-image shorthand."""
    image_file = tmp_path / "image.txt"
    image_file.write_text("python:3.12-slim\n")
    fake_flow = Mock()
    fake_flow.deploy.return_value = _deployment_stub(tags={})

    with (
        patch(
            "kitaru._cli._flows._load_deployable_flow_target",
            return_value=fake_flow,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "build",
                "demo.py:demo_flow",
                "--image",
                f"@{image_file}",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_flow.deploy.assert_called_once_with(
        tags={},
        image=ImageSettings(base_image="python:3.12-slim"),
        publish_default_on_first_deploy=False,
    )
    assert json.loads(capsys.readouterr().out)["command"] == "build"


def test_build_rejects_json_like_but_invalid_image_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--image @file` should still fail for malformed JSON-looking payloads."""
    image_file = tmp_path / "image.json"
    image_file.write_text('{"requirements":["numpy"]')

    with (
        patch("kitaru._cli._flows._load_deployable_flow_target") as mock_loader,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "build",
                "demo.py:demo_flow",
                "--image",
                f"@{image_file}",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    mock_loader.assert_not_called()
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "Invalid JSON for `--image file" in payload["error"]["message"]


def test_build_rejects_invalid_structured_image_before_loading_target(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid structured image settings should fail before the flow target loads."""
    with (
        patch("kitaru._cli._flows._load_deployable_flow_target") as mock_loader,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "build",
                "demo.py:demo_flow",
                "--image",
                '{"requirements":[" "]}',
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    mock_loader.assert_not_called()
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "`--image` must be either a base image string" in payload["error"]["message"]


def test_build_rejects_malformed_inline_image_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Malformed inline JSON should not fall through as a fake base-image string."""
    with (
        patch("kitaru._cli._flows._load_deployable_flow_target") as mock_loader,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "build",
                "demo.py:demo_flow",
                "--image",
                '{"requirements":["numpy"]',
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    mock_loader.assert_not_called()
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "Invalid JSON for `--image`" in payload["error"]["message"]


def test_build_rejects_non_object_input_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--input` must decode to a JSON object."""
    with pytest.raises(SystemExit) as exc_info:
        app(["build", "demo.py:demo_flow", "--input", "[]", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "build"
    assert "must be a JSON object" in payload["error"]["message"]


@pytest.mark.parametrize(
    "reserved_key",
    ["tags", "image", "publish_default_on_first_deploy"],
)
def test_deploy_rejects_reserved_input_keys(
    reserved_key: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Flow inputs must not override deployment-control kwargs like tags/image."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "deploy",
                "demo.py:demo_flow",
                "--input",
                json.dumps({reserved_key: {}, "topic": "AI"}),
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "deploy"
    assert "reserved deployment option key" in payload["error"]["message"]
    assert reserved_key in payload["error"]["message"]


def test_invoke_defaults_to_default_deployment_tag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru invoke FLOW` should route through the default deployment tag."""
    fake_client = Mock()
    fake_client.deployments.invoke.return_value = SimpleNamespace(exec_id="kr-123")
    fake_client.executions.get.return_value = _execution_stub(
        exec_id="kr-123",
        flow_name="demo_flow",
        status=ExecutionStatus.RUNNING,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru._cli._flows.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["invoke", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 0
    fake_client.deployments.invoke.assert_called_once_with(
        flow="demo_flow",
        version=None,
        tag="default",
        selector_source="implicit_default",
        inputs={},
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "invoke"
    assert payload["item"]["selector"] == {"version": None, "tag": "default"}
    assert payload["item"]["exec_id"] == "kr-123"
    track_mock.assert_called_once_with(
        AnalyticsEvent.DEPLOYMENT_INVOKED,
        {"command": "invoke", "has_input": False, "selector": "default"},
    )


def test_invoke_missing_flow_without_selector_omits_default_in_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Implicit default lookup should not pretend the user asked for `default`."""
    fake_client = Mock()
    fake_client.deployments.invoke.side_effect = LookupError(
        "No deployments found for flow 'demo_flow'. Deploy this flow first, "
        "then invoke it by version or tag."
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["invoke", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 1
    fake_client.deployments.invoke.assert_called_once_with(
        flow="demo_flow",
        version=None,
        tag="default",
        selector_source="implicit_default",
        inputs={},
    )
    payload = json.loads(capsys.readouterr().err)
    message = payload["error"]["message"]
    assert message.startswith("No deployments found for flow 'demo_flow'.")
    assert "tag 'default'" not in message


def test_invoke_missing_implicit_default_route_explains_remediation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Implicit default lookup should explain what to do when no route owns it."""
    fake_client = Mock()
    fake_client.deployments.invoke.side_effect = KitaruStateError(
        "Flow 'demo_flow' has deployments, but none is currently routed as "
        "the default deployment. Invoke it with an explicit version or tag, "
        "or move the reserved 'default' tag to the version you want."
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["invoke", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    message = payload["error"]["message"]
    assert "default deployment" in message
    assert "explicit version or tag" in message
    assert "reserved 'default' tag" in message


def test_invoke_explicit_missing_tag_remains_tag_specific(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Explicit tags should still get tag-specific lookup errors."""
    fake_client = Mock()
    fake_client.deployments.invoke.side_effect = LookupError(
        "No deployment found for flow 'demo_flow' with tag 'stable'."
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["invoke", "demo_flow", "--tag", "stable", "-o", "json"])

    assert exc_info.value.code == 1
    fake_client.deployments.invoke.assert_called_once_with(
        flow="demo_flow",
        version=None,
        tag="stable",
        selector_source="tag",
        inputs={},
    )
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["message"] == (
        "No deployment found for flow 'demo_flow' with tag 'stable'."
    )


def test_invoke_selector_conflict_returns_json_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deployment selectors should use the shared mutual-exclusion error."""
    with pytest.raises(SystemExit) as exc_info:
        app(["invoke", "demo_flow", "--version", "1", "--tag", "prod", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "invoke"
    assert "version` and `tag` are mutually exclusive" in payload["error"]["message"]


def test_flow_deployments_curl_json_resolves_default_and_formats_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`flow deployments curl` should generate a pinned curl command as JSON."""
    deployment = _deployment_stub(
        flow="demo_flow",
        version=2,
        deployment_id="7d3176d6-7453-411b-a3f5-91ca5c663d1c",
    )
    fake_client = Mock()
    fake_client.deployments.get.return_value = deployment
    fake_connection = SimpleNamespace(server_url="https://kitaru.example.com/")

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._cli._flows.resolve_connection_config", return_value=fake_connection
        ),
        patch("kitaru._cli._flows.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "curl",
                "demo_flow",
                "--input",
                '{"prompt":"Review data retention."}',
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.deployments.get.assert_called_once_with(
        flow="demo_flow",
        version=None,
        tag="default",
    )
    fake_client.deployments._ensure_deployment_server_runnable.assert_called_once_with(
        deployment,
        operation="curl",
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "flow.deployments.curl"
    item = payload["item"]
    assert item["selector"] == {"version": None, "tag": "default"}
    assert item["resolved_deployment_version"] == 2
    assert item["server_url"] == "https://kitaru.example.com"
    assert item["invoke_url"] == (
        "https://kitaru.example.com/api/v1/pipeline_snapshots/"
        "7d3176d6-7453-411b-a3f5-91ca5c663d1c/runs"
    )
    assert item["request_body"] == {
        "run_configuration": {"parameters": {"prompt": "Review data retention."}}
    }
    assert item["token_env_var"] == "KITARU_SERVER_ACCESS_TOKEN"
    assert item["token_command"] == "kitaru auth token"
    assert 'KITARU_SERVER_ACCESS_TOKEN="$(kitaru auth token)"' in item["curl_command"]
    assert "Authorization: Bearer ${KITARU_SERVER_ACCESS_TOKEN}" in item["curl_command"]
    assert "Review data retention" in item["curl_command"]
    assert "KITARU_AUTH_TOKEN" not in item["curl_command"]
    assert "kat_" not in item["curl_command"]
    assert "This command is pinned to v2" in item["warning"]
    assert item["warning_lines"] == [
        "Resolved demo_flow tag 'default' to deployment version v2.",
        "This command is pinned to v2. Regenerate it if you move the tag.",
    ]
    track_mock.assert_called_once_with(
        AnalyticsEvent.DEPLOYMENT_CURL_GENERATED,
        {
            "command": "flow.deployments.curl",
            "has_input": True,
            "selector": "default",
        },
    )


def test_flow_deployments_curl_rejects_non_runnable_deployments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Curl generation should hard-fail for deployments the server cannot run."""
    deployment = _deployment_stub(
        flow="demo_flow",
        version=2,
        deployment_id="dep-legacy",
    )
    fake_client = Mock()
    fake_client.deployments.get.return_value = deployment
    fake_client.deployments._ensure_deployment_server_runnable.side_effect = (
        KitaruStateError("server cannot run this deployment")
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._cli._flows.resolve_connection_config",
            return_value=SimpleNamespace(server_url="https://kitaru.example.com"),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["flow", "deployments", "curl", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "flow.deployments.curl"
    assert "server cannot run this deployment" in payload["error"]["message"]


@pytest.mark.parametrize(
    ("env", "message_fragment"),
    [
        pytest.param(
            {"KITARU_SERVER_URL": "https://env-kitaru.example.com/"},
            "no Kitaru auth token is available",
            id="missing-auth-token",
        ),
        pytest.param(
            {
                "KITARU_SERVER_URL": "https://env-kitaru.example.com/",
                "KITARU_AUTH_TOKEN": "org-token",
            },
            "no project is active",
            id="missing-project",
        ),
    ],
)
def test_flow_deployments_curl_rejects_invalid_env_backed_connection(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str],
    message_fragment: str,
) -> None:
    """Curl generation should fail before printing a known-broken command."""
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(
        flow="demo_flow",
        version=2,
        deployment_id="dep-env",
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["flow", "deployments", "curl", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "flow.deployments.curl"
    assert message_fragment in payload["error"]["message"]
    fake_client.deployments.get.assert_not_called()


def test_flow_deployments_curl_uses_complete_env_backed_connection_without_login_store(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete env-backed remote connection should still generate curl output."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://env-kitaru.example.com/")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "org-token")
    monkeypatch.setenv("KITARU_PROJECT", "demo-project")
    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(
        flow="demo_flow",
        version=2,
        deployment_id="dep-env",
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru.config.GlobalConfiguration",
            return_value=SimpleNamespace(store=None),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["flow", "deployments", "curl", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 0
    item = json.loads(capsys.readouterr().out)["item"]
    assert item["server_url"] == "https://env-kitaru.example.com"
    assert item["invoke_url"] == (
        "https://env-kitaru.example.com/api/v1/pipeline_snapshots/dep-env/runs"
    )


def test_flow_deployments_curl_text_warns_for_tag_selector(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Text output should warn when a movable tag is resolved to a version."""
    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(
        flow="demo_flow",
        version=3,
        tags={"stable": True},
        deployment_id="dep-123",
    )
    fake_connection = SimpleNamespace(server_url="https://kitaru.example.com")

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._cli._flows.resolve_connection_config", return_value=fake_connection
        ),
        patch("kitaru._cli._flows.track", return_value=True),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["flow", "deployments", "curl", "demo_flow", "--tag", "stable"])

    assert exc_info.value.code == 0
    fake_client.deployments.get.assert_called_once_with(
        flow="demo_flow",
        version=None,
        tag="stable",
    )
    output = capsys.readouterr().out
    assert (
        "# Resolved demo_flow tag 'stable' to deployment version v3.\n"
        "# This command is pinned to v3. Regenerate it if you move the tag.\n"
    ) in output
    assert 'KITARU_SERVER_ACCESS_TOKEN="$(kitaru auth token)"' in output
    assert "curl -sS -X POST" in output
    assert "${KITARU_SERVER_ACCESS_TOKEN}" in output


def test_flow_deployments_curl_version_selector_uses_empty_body_without_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Exact-version curl generation should not emit tag-pinning warnings."""
    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(
        flow="demo_flow",
        version=5,
        deployment_id="dep-5",
    )
    fake_connection = SimpleNamespace(server_url="https://kitaru.example.com")

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._cli._flows.resolve_connection_config", return_value=fake_connection
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "curl",
                "demo_flow",
                "--version",
                "5",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.deployments.get.assert_called_once_with(
        flow="demo_flow",
        version=5,
        tag=None,
    )
    item = json.loads(capsys.readouterr().out)["item"]
    assert item["selector"] == {"version": 5, "tag": None}
    assert item["request_body"] == {}
    assert "warning" not in item
    assert "-d '{}'" in item["curl_command"]


def test_flow_deployments_curl_single_quotes_literal_url_parts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Literal URL pieces should not allow shell expansion when pasted."""
    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(
        flow="demo_flow",
        version=1,
        deployment_id="dep-safe",
    )
    fake_connection = SimpleNamespace(
        server_url="https://kitaru.example.com/$(touch /tmp/pwn)"
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._cli._flows.resolve_connection_config", return_value=fake_connection
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["flow", "deployments", "curl", "demo_flow", "-o", "json"])

    assert exc_info.value.code == 0
    command = json.loads(capsys.readouterr().out)["item"]["curl_command"]
    assert "'https://kitaru.example.com/$(touch /tmp/pwn)/api/v1/" in command
    assert '"https://kitaru.example.com/$(touch /tmp/pwn)' not in command
    assert "Authorization: Bearer ${KITARU_SERVER_ACCESS_TOKEN}" in command


def test_flow_deployments_curl_reads_input_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--input @file` should be accepted when generating curl commands."""
    input_file = tmp_path / "inputs.json"
    input_file.write_text('{"topic":"cats"}')
    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(
        flow="demo_flow",
        version=1,
        deployment_id="dep-file",
    )
    fake_connection = SimpleNamespace(server_url="https://kitaru.example.com")

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._cli._flows.resolve_connection_config", return_value=fake_connection
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "curl",
                "demo_flow",
                "--input",
                f"@{input_file}",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    item = json.loads(capsys.readouterr().out)["item"]
    assert item["request_body"] == {
        "run_configuration": {"parameters": {"topic": "cats"}}
    }


def test_flow_list_json_groups_deployment_backed_flows(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru flow list -o json` should summarize deployments by flow."""
    fake_client = Mock()
    fake_client.deployments.list.return_value = [
        _deployment_stub(flow="alpha", version=1),
        _deployment_stub(flow="alpha", version=2, tags={"prod": False}),
        _deployment_stub(flow="beta", version=1),
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["flow", "list", "-o", "json"])

    assert exc_info.value.code == 0
    fake_client.deployments.list.assert_called_once_with()
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "flow.list"
    assert payload["count"] == 2
    assert payload["items"][0]["flow"] == "alpha"
    assert payload["items"][0]["latest_version"] == 2


def test_flow_deployments_logs_explicit_exec_id_skips_deployment_resolution(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--exec-id` should be a true override for deleted or moved deployments."""
    entry = LogEntry(message="Starting deployment", level="INFO")
    fake_client = Mock()
    fake_client.deployments.get.side_effect = AssertionError(
        "deployment selector should not be resolved when --exec-id is provided"
    )
    fake_client.executions.logs.return_value = [entry]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "logs",
                "demo_flow",
                "--exec-id",
                "kr-explicit",
                "--version",
                "999",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.deployments.get.assert_not_called()
    fake_client.executions.list.assert_not_called()
    fake_client.executions.logs.assert_called_once_with(
        "kr-explicit",
        checkpoint=None,
        source="step",
        limit=None,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "flow.deployments.logs"
    assert payload["count"] == 1


def test_flow_deployments_logs_searches_all_flow_executions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deployment log lookup should not stop after an arbitrary first page."""
    deployment = _deployment_stub(
        flow="demo_flow",
        version=7,
        deployment_id="dep-target",
    )
    unrelated = SimpleNamespace(
        exec_id="kr-unrelated",
        metadata={"kitaru_deployment_id": "dep-other"},
    )
    matching = SimpleNamespace(
        exec_id="kr-matching",
        metadata={"kitaru_deployment_id": "dep-target"},
    )
    entry = LogEntry(message="Older matching deployment", level="INFO")
    fake_client = Mock()
    fake_client.deployments.get.return_value = deployment
    fake_client.executions.list.return_value = [unrelated, matching]
    fake_client.executions.logs.return_value = [entry]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "logs",
                "demo_flow",
                "--version",
                "7",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.list.assert_called_once_with(flow="demo_flow")
    fake_client.executions.logs.assert_called_once_with(
        "kr-matching",
        checkpoint=None,
        source="step",
        limit=None,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1


def test_flow_deployments_logs_follow_json_uses_command_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deployment log follow JSONL should preserve the invoking command name."""
    running = _execution_stub(
        exec_id="kr-123",
        flow_name="demo_flow",
        status=ExecutionStatus.RUNNING,
    )
    completed = _execution_stub(
        exec_id="kr-123",
        flow_name="demo_flow",
        status=ExecutionStatus.COMPLETED,
    )
    entry = LogEntry(message="Starting deployment", level="INFO")
    fake_client = Mock()
    fake_client.deployments.get.return_value = _deployment_stub(flow="demo_flow")
    fake_client.executions.logs.side_effect = [[entry], [entry]]
    fake_client.executions.get.side_effect = [running, completed]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru.cli.time.sleep") as sleep_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "logs",
                "demo_flow",
                "--exec-id",
                "kr-123",
                "--follow",
                "-o",
                "json",
                "--interval",
                "0.01",
            ]
        )

    assert exc_info.value.code == 0
    sleep_mock.assert_called_once_with(0.01)
    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert {line["command"] for line in lines} == {"flow.deployments.logs"}
    assert lines[0]["event"] == "log"
    assert lines[-1]["event"] == "terminal"


def test_flow_deployments_delete_calls_public_api(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deployment deletion should go through `client.deployments.delete`."""
    fake_client = Mock()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru._cli._flows.track", return_value=True) as track_mock,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "delete",
                "demo_flow",
                "--version",
                "2",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.deployments.delete.assert_called_once_with(flow="demo_flow", version=2)
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"] == {"flow": "demo_flow", "version": 2, "deleted": True}
    track_mock.assert_called_once_with(
        AnalyticsEvent.DEPLOYMENT_DELETED,
        {"command": "flow.deployments.delete", "selector": "version"},
    )


def test_flow_deployments_delete_surfaces_public_api_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Exclusive-tag delete protection should surface from the public API."""
    fake_client = Mock()
    fake_client.deployments.delete.side_effect = KitaruStateError(
        "Cannot delete deployment while it holds exclusive tag(s): default."
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "flow",
                "deployments",
                "delete",
                "demo_flow",
                "--version",
                "1",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["command"] == "flow.deployments.delete"
    assert "exclusive tag" in payload["error"]["message"]


def test_flow_tag_and_untag_call_public_apis(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Tag management commands should call public deployment APIs."""
    fake_client = Mock()
    fake_client.deployments.tag.return_value = _deployment_stub(tags={"prod": True})
    fake_client.deployments.untag.return_value = _deployment_stub(tags={})

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru._cli._flows.track", return_value=True),
        pytest.raises(SystemExit) as tag_exit,
    ):
        app(
            [
                "flow",
                "tag",
                "demo_flow",
                "prod",
                "--version",
                "1",
                "--exclusive",
                "-o",
                "json",
            ]
        )

    assert tag_exit.value.code == 0
    fake_client.deployments.tag.assert_called_once_with(
        flow="demo_flow",
        version=1,
        tag="prod",
        exclusive=True,
    )
    assert json.loads(capsys.readouterr().out)["command"] == "flow.tag"

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        patch("kitaru._cli._flows.track", return_value=True),
        pytest.raises(SystemExit) as untag_exit,
    ):
        app(["flow", "untag", "demo_flow", "prod", "--version", "1", "-o", "json"])

    assert untag_exit.value.code == 0
    fake_client.deployments.untag.assert_called_once_with(
        flow="demo_flow",
        version=1,
        tag="prod",
    )
    assert json.loads(capsys.readouterr().out)["command"] == "flow.untag"


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
        llm_usage_summary={
            "usage_record_count": 2,
            "incurred_usage_record_count": 1,
            "reused_usage_record_count": 1,
            "total_tokens": 42,
            "display_cost_usd": 0.125,
            "actual_cost_usd": 0.1,
            "estimated_cost_usd": 0.025,
        },
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
    assert "LLM usage: 2 usage records (1 incurred, 1 reused), 42 tokens" in output


def test_executions_get_renders_malformed_llm_usage_summary_honestly(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Malformed LLM usage summary numbers should not render as real zeroes."""
    execution = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.COMPLETED,
        llm_usage_summary={
            "usage_record_count": "not-an-int",
            "incurred_usage_record_count": True,
            "reused_usage_record_count": None,
            "total_tokens": "not-an-int",
            "display_cost_usd": "not-a-number",
            "actual_cost_usd": float("nan"),
            "estimated_cost_usd": None,
        },
    )
    fake_client = Mock()
    fake_client.executions.get.return_value = execution

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "get", "kr-123"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "LLM usage: summary metadata is malformed" in output
    assert "0 calls (0 incurred, 0 reused), 0 tokens" not in output
    assert "display cost $0.000000" not in output


def test_executions_get_renders_valid_zero_llm_usage_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A complete zero summary should still render as real zero usage."""
    execution = _execution_stub(
        exec_id="kr-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.COMPLETED,
        llm_usage_summary={
            "usage_record_count": 0,
            "incurred_usage_record_count": 0,
            "reused_usage_record_count": 0,
            "total_tokens": 0,
            "display_cost_usd": 0.0,
            "actual_cost_usd": 0.0,
            "estimated_cost_usd": 0.0,
        },
    )
    fake_client = Mock()
    fake_client.executions.get.return_value = execution

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "get", "kr-123"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "LLM usage: 0 usage records (0 incurred, 0 reused), 0 tokens" in output
    assert "display cost $0.000000" in output


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
    assert "Started" in header_lines[1]
    assert "Ended" in header_lines[1]
    assert "Stack" in header_lines[1]
    assert "2026-03-07 10:00:00" in output
    assert "2026-03-07 10:01:00" in output
    assert "kr-200" in output
    assert "content_pipeline" in output
    assert "waiting" in output
    assert "prod" in output


def test_executions_list_uses_default_page_size() -> None:
    """`kitaru executions list` should default to the first 20-item page."""
    fake_client = Mock()
    fake_client.executions.list.return_value = []

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "list"])

    assert exc_info.value.code == 0
    fake_client.executions.list.assert_called_once_with(
        status=None,
        flow=None,
        page=1,
        size=20,
    )


def test_executions_list_accepts_page_and_size() -> None:
    """`kitaru executions list` should pass explicit page/size to the client."""
    fake_client = Mock()
    fake_client.executions.list.return_value = []

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "list", "--page", "2", "--size", "10"])

    assert exc_info.value.code == 0
    fake_client.executions.list.assert_called_once_with(
        status=None,
        flow=None,
        page=2,
        size=10,
    )


def _statistics_with_status_groups(
    *groups: tuple[str, int],
    truncated: bool = False,
) -> ExecutionStatistics:
    """Build execution statistics grouped by status for CLI tests."""
    return ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(keys={"status": status}, execution_count=count)
            for status, count in groups
        ],
        truncated=truncated,
    )


def test_executions_statistics_forwards_filters_and_repeatable_options() -> None:
    """`kitaru executions statistics` should delegate to the SDK surface."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"status": "failed", "flow_id": "flow-123"},
                execution_count=2,
                metrics={"duration_avg": 4.2},
            )
        ],
        truncated=False,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--group-by",
                "status",
                "--group-by",
                "flow",
                "--metric",
                "duration_avg:duration:avg",
                "--flow",
                "content_pipeline",
                "--status",
                "failed",
                "--stack",
                "prod",
                "--tag",
                "nightly",
                "--tag",
                "customer-facing",
                "--max-groups",
                "25",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.statistics.assert_called_once_with(
        group_by=["status", "flow"],
        metrics=["duration_avg:duration:avg"],
        flow="content_pipeline",
        status="failed",
        stack="prod",
        tags=["nightly", "customer-facing"],
        max_groups=25,
    )


def test_executions_statistics_forwards_llm_shortcuts_and_emits_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """LLM shortcut metric strings should pass through to the SDK unchanged."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"flow_id": "flow-123"},
                execution_count=3,
                metrics={"llm_display_cost": 0.42, "llm_total_tokens": 128.0},
            )
        ],
        truncated=False,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--flow",
                "my_flow",
                "--group-by",
                "flow",
                "--metric",
                "llm_display_cost",
                "--metric",
                "llm_total_tokens",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.statistics.assert_called_once_with(
        group_by=["flow"],
        metrics=["llm_display_cost", "llm_total_tokens"],
        flow="my_flow",
        status=None,
        stack=None,
        tags=None,
        max_groups=1000,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["groups"] == [
        {
            "keys": {"flow_id": "flow-123"},
            "execution_count": 3,
            "metrics": {"llm_display_cost": 0.42, "llm_total_tokens": 128.0},
        }
    ]


def test_executions_statistics_text_orders_llm_shortcut_columns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Text metric columns should follow requested shortcut order."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"status": "completed"},
                execution_count=3,
                metrics={"llm_total_tokens": 128.0, "llm_display_cost": 0.42},
            )
        ],
        truncated=False,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--group-by",
                "status",
                "--metric",
                "llm_display_cost",
                "--metric",
                "llm_total_tokens",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert output.index("Llm Display Cost") < output.index("Llm Total Tokens")
    assert "0.42" in output
    assert "128.0" in output


def test_executions_statistics_rejects_invalid_llm_shortcut_like_metric(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid shortcut-like metrics should use the normal CLI error path."""
    fake_client = Mock()

    def _statistics(**kwargs: Any) -> ExecutionStatistics:
        normalize_execution_statistics_metrics(kwargs["metrics"])
        raise AssertionError("invalid metric should fail before statistics return")

    fake_client.executions.statistics.side_effect = _statistics

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics", "--metric", "llm_not_a_real_shortcut"])

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        "Unsupported execution statistics metric 'llm_not_a_real_shortcut'"
        in captured.err
    )


def test_executions_statistics_metric_help_lists_llm_shortcuts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The metric help text should advertise the common LLM shortcuts."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "statistics", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    for shortcut in LLM_EXECUTION_STATISTICS_METRIC_SHORTCUTS_DISPLAY.split(", "):
        assert shortcut in output


def test_executions_statistics_accepts_pagination_without_forwarding_it() -> None:
    """Statistics pagination should page CLI output, not SDK queries."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = _statistics_with_status_groups(
        ("completed", 12),
        ("failed", 2),
        ("running", 1),
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--group-by",
                "status",
                "--page",
                "2",
                "--size",
                "1",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.statistics.assert_called_once_with(
        group_by=["status"],
        metrics=[],
        flow=None,
        status=None,
        stack=None,
        tags=None,
        max_groups=1000,
    )


def test_executions_statistics_pages_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Statistics text output should contain only the requested group page."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = _statistics_with_status_groups(
        ("completed", 12),
        ("failed", 2),
        ("running", 1),
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--group-by",
                "status",
                "--page",
                "2",
                "--size",
                "1",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "completed" not in output
    assert "failed" in output
    assert "running" not in output
    assert "Page 2 (size 1, showing 1 of 3)" in output


def test_executions_statistics_renders_grouped_text(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Grouped statistics should render dynamic columns plus execution counts."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"status": "completed"},
                execution_count=12,
                metrics={"duration_avg": 5.5},
            ),
            ExecutionStatisticsGroup(keys={"status": "failed"}, execution_count=2),
            ExecutionStatisticsGroup(keys={"status": "running"}, execution_count=1),
        ],
        truncated=False,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics", "--group-by", "status"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru execution statistics" in output
    assert "Status" in output
    assert "Executions" in output
    assert "Duration Avg" in output
    assert "completed" in output
    assert "failed" in output
    assert "running" in output
    assert "12" in output
    assert "5.5" in output
    assert "2" in output
    assert "1" in output


def test_executions_statistics_table_uses_requested_metric_order() -> None:
    """Metric columns should follow request order, not response dict order."""
    statistics = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"status": "completed"},
                execution_count=12,
                metrics={"cost_sum": 2.5, "duration_avg": 5.5},
            )
        ],
        truncated=False,
    )

    columns, rows = _execution_statistics_table(
        statistics,
        requested_metric_names=["duration_avg", "cost_sum"],
    )

    assert columns == ["Status", "Executions", "Duration Avg", "Cost Sum"]
    assert rows == [["completed", "12", "5.5", "2.5"]]


def test_executions_statistics_renders_truncation_note(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Text statistics output should tell users when rows are truncated."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(keys={"status": "completed"}, execution_count=7)
        ],
        truncated=True,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics", "--group-by", "status", "--max-groups", "1"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Results truncated at --max-groups 1" in output
    assert "Narrow filters or increase --max-groups" in output


def test_executions_statistics_renders_global_text(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No-grouping statistics should render one global execution count."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[ExecutionStatisticsGroup(keys={}, execution_count=18)],
        truncated=False,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru execution statistics" in output
    assert "Executions" in output
    assert "18" in output


@pytest.mark.parametrize(
    "output_args",
    (["-o", "json"], ["--output", "json"]),
)
def test_executions_statistics_emits_json(
    capsys: pytest.CaptureFixture[str],
    output_args: list[str],
) -> None:
    """JSON statistics output should use the standard single-item envelope."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"status": "completed", "day": "2026-05-30"},
                execution_count=7,
                metrics={"duration_avg": 9.5},
            ),
            ExecutionStatisticsGroup(
                keys={"status": "failed", "day": "2026-05-30"},
                execution_count=2,
                metrics={"duration_avg": 3.0},
            ),
        ],
        truncated=True,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--group-by",
                "time:day",
                "--group-by",
                "status",
                *output_args,
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "executions.statistics",
        "item": {
            "groups": [
                {
                    "keys": {"status": "completed", "day": "2026-05-30"},
                    "execution_count": 7,
                    "metrics": {"duration_avg": 9.5},
                },
                {
                    "keys": {"status": "failed", "day": "2026-05-30"},
                    "execution_count": 2,
                    "metrics": {"duration_avg": 3.0},
                },
            ],
            "truncated": True,
            "group_count": 2,
        },
    }


def test_executions_statistics_pages_json_without_pagination_metadata(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Paged JSON statistics should keep the single-item command envelope."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = _statistics_with_status_groups(
        ("completed", 12),
        ("failed", 2),
        ("running", 1),
        truncated=True,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "statistics",
                "--group-by",
                "status",
                "--page",
                "2",
                "--size",
                "1",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"command", "item"}
    assert "items" not in payload
    item = payload["item"]
    assert "page" not in item
    assert "size" not in item
    assert "total_count" not in item
    assert item == {
        "groups": [
            {
                "keys": {"status": "failed"},
                "execution_count": 2,
                "metrics": {},
            }
        ],
        "truncated": True,
        "group_count": 1,
    }


def test_executions_statistics_uses_default_size_for_partial_pagination(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Providing only --page should use the shared default page size."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(keys={"status": f"status-{i}"}, execution_count=i)
            for i in range(25)
        ],
        truncated=False,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics", "--page", "2"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "status-19" not in output
    assert "status-20" in output
    assert "status-24" in output
    assert "Page 2 (size 20, showing 5 of 25)" in output


def test_executions_statistics_uses_default_page_for_partial_pagination(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Providing only --size should use the first page."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = _statistics_with_status_groups(
        ("completed", 12),
        ("failed", 2),
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics", "--size", "1"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "completed" in output
    assert "failed" not in output
    assert "Page 1 (size 1, showing 1 of 2)" in output


@pytest.mark.parametrize(
    ("pagination_args", "expected_message"),
    [
        (["--page", "0"], "`--page` must be >= 1."),
        (["--size", "0"], "`--size` must be >= 1."),
    ],
)
def test_executions_statistics_pagination_validation_json_error(
    capsys: pytest.CaptureFixture[str],
    pagination_args: list[str],
    expected_message: str,
) -> None:
    """Invalid statistics pagination should respect JSON error output."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "statistics", *pagination_args, "--output", "json"])

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "command": "executions.statistics",
        "error": {"message": expected_message},
    }


def test_executions_statistics_out_of_range_page_is_empty(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Out-of-range statistics pages should be successful empty responses."""
    fake_client = Mock()
    fake_client.executions.statistics.return_value = _statistics_with_status_groups(
        ("completed", 12),
        ("failed", 2),
        ("running", 1),
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "statistics", "--page", "99", "--size", "20"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "none found" in output
    assert "Page 99 (size 20, showing 0 of 3)" in output


def test_executions_statistics_rejects_invalid_max_groups(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI max-groups validation should fail before calling the SDK."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "statistics", "--max-groups", "0"])

    assert exc_info.value.code == 1
    error = capsys.readouterr().err
    assert "--max-groups" in error
    assert "between 1 and 10000" in error


def test_executions_list_rejects_limit_with_page(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Legacy `--limit` should not compose with explicit pagination."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "list", "--limit", "5", "--page", "2"])

    assert exc_info.value.code == 1
    assert "cannot be combined" in capsys.readouterr().err


def test_list_pagination_validation_json_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid list pagination should respect JSON error output."""
    with pytest.raises(SystemExit) as exc_info:
        app(["model", "list", "--page", "0", "--output", "json"])

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "command": "model.list",
        "error": {
            "message": "`--page` must be >= 1.",
        },
    }


def test_executions_list_rejects_limit_with_explicit_default_page(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--limit` + any explicit `--page`/`--size` should error, even at defaults."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "list", "--limit", "5", "--page", "1"])

    assert exc_info.value.code == 1
    assert "cannot be combined" in capsys.readouterr().err


def test_executions_list_emits_pagination_note_when_full_page_returned(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A returned page at full size should advertise that more items may exist."""
    fake_client = Mock()
    fake_client.executions.list.return_value = [
        _execution_stub(
            exec_id=f"kr-{i:02d}", flow_name="f", status=ExecutionStatus.COMPLETED
        )
        for i in range(20)
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "list"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Page 1 (size 20, returned 20)" in output
    assert "there may be more items" in output


def test_executions_list_suppresses_pagination_note_on_partial_first_page(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Short first page should suppress the pagination note entirely."""
    fake_client = Mock()
    fake_client.executions.list.return_value = [
        _execution_stub(
            exec_id=f"kr-{i}", flow_name="f", status=ExecutionStatus.COMPLETED
        )
        for i in range(3)
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "list"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Page " not in output


def test_executions_list_note_has_no_more_items_suffix_on_short_later_page(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A later page with fewer results than size is definitively the tail."""
    fake_client = Mock()
    fake_client.executions.list.return_value = [
        _execution_stub(
            exec_id=f"kr-{i}", flow_name="f", status=ExecutionStatus.COMPLETED
        )
        for i in range(7)
    ]

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "list", "--page", "2", "--size", "20"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Page 2 (size 20, returned 7)" in output
    assert "there may be more items" not in output


def test_secrets_list_past_end_does_not_claim_none_found(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Paging past the end of a non-empty secret list must not say 'none found'."""
    secret_a = SimpleNamespace(name="alpha", id="secret-a", private=False)
    secret_b = SimpleNamespace(name="beta", id="secret-b", private=False)
    fake_client = Mock()
    fake_client.list_secrets.return_value = SimpleNamespace(
        items=[secret_a, secret_b],
        total_pages=1,
        max_size=2,
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "list", "--page", "9", "--size", "1"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "no items on page 9" in output
    assert "none found" not in output
    assert "Page 9 (size 1, showing 0 of 2)" in output


def test_stack_list_paginates_text_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Text-mode stack list should slice to the requested window."""
    stacks = [
        SimpleNamespace(id="stack-a-id", name="alpha", is_active=False),
        SimpleNamespace(id="stack-b-id", name="beta", is_active=True),
        SimpleNamespace(id="stack-c-id", name="gamma", is_active=False),
    ]

    with (
        patch("kitaru.cli.get_available_stacks", return_value=stacks),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["stack", "list", "--page", "2", "--size", "1"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "beta: stack-b-id (active)" in output
    assert "alpha" not in output
    assert "gamma" not in output
    assert "Page 2 (size 1, showing 1 of 3)" in output


def test_stack_list_paginates_json_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """JSON-mode stack list should slice the same window as text mode."""
    entries = [
        SimpleNamespace(
            stack=SimpleNamespace(id="stack-a-id", name="alpha", is_active=False),
            is_managed=False,
        ),
        SimpleNamespace(
            stack=SimpleNamespace(id="stack-b-id", name="beta", is_active=True),
            is_managed=False,
        ),
        SimpleNamespace(
            stack=SimpleNamespace(id="stack-c-id", name="gamma", is_active=False),
            is_managed=True,
        ),
    ]

    with (
        patch("kitaru.cli._list_stack_entries", return_value=entries),
        patch(
            "kitaru._cli._stacks.serialize_stack",
            side_effect=lambda s, *, is_managed: {
                "id": s.id,
                "name": s.name,
                "is_active": s.is_active,
                "is_managed": is_managed,
            },
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["stack", "list", "--page", "2", "--size", "1", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"command", "items", "count"}
    assert payload["command"] == "stack.list"
    assert payload["count"] == 1
    assert payload["items"][0]["name"] == "beta"


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


def test_executions_replay_parses_unified_overrides_and_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions replay` should parse new override flags."""
    fake_client = Mock()
    fake_client.executions.replay.return_value = _replay_submission_stub()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "replay",
                "kr-111",
                "--at",
                "write_summary",
                "--flow-overrides",
                '{"topic":"new topic"}',
                "--checkpoint-overrides",
                '{"research":{"output":"edited"}}',
                "--invocation-overrides",
                '{"call-1":{"model":"gpt-5-nano"}}',
                "--skip",
                "lookup_policy_tool,write_draft",
                "--tag",
                "best-replay-june",
                "--wait",
                "--on-error",
                "fail",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.replay.assert_called_once_with(
        ["kr-111"],
        at="write_summary",
        flow_overrides={"topic": "new topic"},
        checkpoint_overrides={"research": {"output": "edited"}},
        invocation_overrides={"call-1": {"model": "gpt-5-nano"}},
        skip=["lookup_policy_tool", "write_draft"],
        tag="best-replay-june",
        wait=True,
        on_error="fail",
    )
    output = capsys.readouterr().out
    assert "Replayed execution: kr-222" in output
    assert "Status: submitted" in output
    assert "Compare original vs replay:" in output
    assert "compare?executions=kr-111,kr-222" in output


def test_executions_replay_multiple_ids_json_envelope(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Multiple parent IDs should pass as a list and emit ReplaySubmission JSON."""
    fake_client = Mock()
    fake_client.executions.replay.return_value = _replay_submission_stub(
        at="lookup_policy_tool",
        wait=False,
        results=[
            ReplayResultRow(
                original_exec_ref="kr-a",
                original_exec_id="kr-a",
                replay_exec_id="kr-a-replay",
                status="submitted",
            ),
            ReplayResultRow(
                original_exec_ref="kr-b",
                original_exec_id="kr-b",
                replay_exec_id="kr-b-replay",
                status="submitted",
            ),
        ],
        compare_url="http://localhost:8237/compare?executions=kr-a,kr-a-replay,kr-b,kr-b-replay",
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "replay",
                "kr-a",
                "kr-b",
                "--at",
                "lookup_policy_tool",
                "--no-wait",
                "--on-error",
                "collect",
                "-o",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.executions.replay.assert_called_once_with(
        ["kr-a", "kr-b"],
        at="lookup_policy_tool",
        flow_overrides=None,
        checkpoint_overrides=None,
        invocation_overrides=None,
        skip=None,
        tag=None,
        wait=False,
        on_error="collect",
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "executions.replay"
    assert payload["item"]["submission_id"] == "rs-test"
    assert payload["item"]["summary"] == {
        "submitted": 2,
        "completed": 0,
        "failed": 0,
        "skipped": 0,
    }
    assert payload["item"]["results"][0]["replay_exec_id"] == "kr-a-replay"


def test_executions_replay_omitted_wait_forwards_none(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Omitting wait should let the SDK choose the single/batch default."""
    fake_client = Mock()
    fake_client.executions.replay.return_value = _replay_submission_stub()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "replay", "kr-111", "--at", "write_summary"])

    assert exc_info.value.code == 0
    assert fake_client.executions.replay.call_args.kwargs["wait"] is None


@pytest.mark.parametrize(
    ("flag", "option_name"),
    [
        ("--flow-overrides", "--flow-overrides"),
        ("--checkpoint-overrides", "--checkpoint-overrides"),
        ("--invocation-overrides", "--invocation-overrides"),
    ],
)
def test_executions_replay_rejects_invalid_override_json(
    capsys: pytest.CaptureFixture[str],
    flag: str,
    option_name: str,
) -> None:
    """Override JSON parse errors should name the exact failed option."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "replay",
                "kr-111",
                "--at",
                "write_summary",
                flag,
                "{invalid",
            ]
        )

    assert exc_info.value.code == 1
    assert f"Invalid JSON for `{option_name}`" in capsys.readouterr().err


def test_executions_replay_loads_exec_ids_from_file(
    tmp_path: Path,
) -> None:
    """`--ids-file` should accept a JSON object with exec_ids."""
    ids_path = tmp_path / "ids.json"
    ids_path.write_text('{"exec_ids":["kr-a","kr-b"]}')
    fake_client = Mock()
    fake_client.executions.replay.return_value = _replay_submission_stub()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "executions",
                "replay",
                "--ids-file",
                str(ids_path),
                "--at",
                "lookup_policy_tool",
            ]
        )

    assert exc_info.value.code == 0
    assert fake_client.executions.replay.call_args.args[0] == ["kr-a", "kr-b"]


def test_executions_replay_ids_file_rejects_non_string_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--ids-file` should fail clearly for malformed execution ID lists."""
    ids_path = tmp_path / "ids.json"
    ids_path.write_text('{"exec_ids":[{"bad":"shape"}]}')

    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "replay",
                "--ids-file",
                str(ids_path),
                "--at",
                "lookup_policy_tool",
            ]
        )

    assert exc_info.value.code == 1
    assert "must contain only string execution IDs" in capsys.readouterr().err


def test_executions_replay_help_hides_old_flags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Prototype replay flags should no longer be public CLI options."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "replay", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    for old_flag in ("--args", "--input", "--mock-output", "--tool", "--llm-model"):
        assert old_flag not in output


def test_executions_replay_rejects_old_args_flag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Old replay flags should not be accepted by the command parser."""
    with pytest.raises(SystemExit) as exc_info:
        app(
            [
                "executions",
                "replay",
                "kr-111",
                "--at",
                "write_summary",
                "--args",
                "{}",
            ]
        )

    assert exc_info.value.code != 0
    assert "--args" in capsys.readouterr().err


def test_executions_replay_many_is_not_registered(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`executions replay-many` should be removed from the public CLI."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "--help"])

    assert exc_info.value.code == 0
    assert "replay-many" not in capsys.readouterr().out


def test_executions_diff_matrix_json_uses_new_command_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`executions diff-matrix` should replace the old diff-cohort command."""
    with (
        patch("kitaru.diff.diff_cohort", return_value=object()) as diff_cohort,
        patch("kitaru.diff.serialize_cohort_diff", return_value={"rows": []}),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["executions", "diff-matrix", "kr-a", "kr-b", "-o", "json"])

    assert exc_info.value.code == 0
    diff_cohort.assert_called_once_with(["kr-a", "kr-b"])
    assert json.loads(capsys.readouterr().out) == {
        "command": "executions.diff_matrix",
        "item": {"rows": []},
    }


def test_executions_diff_cohort_is_not_registered(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The old diff-cohort public alias should be gone."""
    with pytest.raises(SystemExit) as exc_info:
        app(["executions", "--help"])

    assert exc_info.value.code == 0
    assert "diff-cohort" not in capsys.readouterr().out


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


def test_executions_resume_accepts_exec_id_option(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru executions resume --exec-id` should match positional behavior."""
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
        app(["executions", "resume", "--exec-id", "kr-123"])

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
    assert "Project: demo-project" in output
    assert "Active project" not in output


def test_login_remote_without_project_does_not_print_project(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Remote login without --project should not guess or print a project."""
    with (
        patch("kitaru.cli.login_to_server") as mock_login,
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="https://example.com",
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["login", "https://example.com/"])

    assert exc_info.value.code == 0
    mock_login.assert_called_once_with(
        "https://example.com/",
        api_key=None,
        refresh=False,
        project=None,
        no_verify_ssl=False,
        ssl_ca_cert=None,
        timeout=60,
    )
    output = capsys.readouterr().out
    assert "Connected to Kitaru server: https://example.com" in output
    assert "Project:" not in output
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
    captured = capsys.readouterr()
    # Warnings go to stderr in non-interactive mode
    combined = captured.out + captured.err
    assert "Auth environment variables are active (KITARU_SERVER_URL)." in combined
    assert "runtime connections may still use those environment variables" in combined
    assert "Connected to local Kitaru server." in combined


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
    captured = capsys.readouterr()
    # Warnings go to stderr in non-interactive mode
    combined = captured.out + captured.err
    assert "Disconnecting from remote server: https://prod.kitaru.io" in combined
    assert "Connected to local Kitaru server." in combined


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


def test_logout_resets_remote_connection_when_daemon_stop_fails() -> None:
    """Remote logout should reset persisted state even if daemon stop fails."""
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
            side_effect=RuntimeError("daemon stop failed"),
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
    assert result.local_server_stopped is False
    assert str(result) == "Logged out from Kitaru server: https://example.com"


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

    fake_gc.set_default_store.assert_called_once_with()
    assert result.mode == "local_server"
    assert result.local_fallback_available is True
    assert result.local_server_stopped is True
    assert str(result) == "Logged out from the local Kitaru server."


def test_logout_local_server_branch_clears_store_on_missing_fallback() -> None:
    """Local-server logout should clear persisted state if local fallback is absent."""
    fake_gc = Mock()
    fake_gc.set_default_store.side_effect = ImportError("sqlalchemy missing")

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

    fake_gc.set_default_store.assert_called_once_with()
    assert fake_gc.store is None
    assert fake_gc._zen_store is None
    assert fake_gc.active_stack_id is None
    assert fake_gc.active_project_id is None
    assert fake_gc._active_stack is None
    assert fake_gc._active_project is None
    fake_gc._write_config.assert_called_once_with()
    assert result.mode == "local_server"
    assert result.local_fallback_available is False
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
            "kitaru._inspection_runtime.get_local_server",
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
            "kitaru._inspection_runtime.get_local_server",
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
            "anthropic-test-key-456",
        ]
    )

    assert parsed == {
        "OPENAI_API_KEY": "sk-123",
        "ANTHROPIC_API_KEY": "anthropic-test-key-456",
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
                "--ANTHROPIC_API_KEY=anthropic-test-key-123",
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
    """`kitaru secrets set` should create public secrets by default."""
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
        private=False,
    )
    output = capsys.readouterr().out
    assert "Created secret: openai-creds" in output
    assert "Secret ID: secret-id" in output


def test_secrets_set_creates_private_secret_when_requested(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets set --private` should opt into private creation."""
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
                "--private",
                "--OPENAI_API_KEY=sk-123",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.create_secret.assert_called_once_with(
        name="openai-creds",
        values={"OPENAI_API_KEY": "sk-123"},
        private=True,
    )
    assert "Created secret: openai-creds" in capsys.readouterr().out


def test_secrets_set_accepts_private_after_assignments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--private` should remain a CLI flag after leading-hyphen assignments."""
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
                "--private",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.create_secret.assert_called_once_with(
        name="openai-creds",
        values={"OPENAI_API_KEY": "sk-123"},
        private=True,
    )
    assert "Created secret: openai-creds" in capsys.readouterr().out


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
    fake_client.create_secret.assert_called_once_with(
        name="openai-creds",
        values={"OPENAI_API_KEY": "sk-123"},
        private=False,
    )
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
        private=False,
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
    assert payload["item"]["visibility"] == "public"


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
    calls = fake_client.list_secrets.call_args_list
    assert len(calls) == 2
    backend_scan_size = calls[0].kwargs["size"]
    assert calls == [
        call(page=1, size=backend_scan_size),
        call(page=2, size=backend_scan_size),
    ]
    output = capsys.readouterr().out
    assert "Kitaru secrets" in output
    assert "alpha: secret-a (private)" in output
    assert "zeta: secret-z (public)" in output
    assert output.index("alpha: secret-a (private)") < output.index(
        "zeta: secret-z (public)"
    )


def test_secrets_list_uses_stable_backend_page_size(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Backend scan pagination should not switch sizes after the first page."""
    secret_z = SimpleNamespace(name="zeta", id="secret-z", private=False)
    secret_a = SimpleNamespace(name="alpha", id="secret-a", private=True)
    observed_sizes: list[int] = []

    def list_secrets(*, page: int, size: int | None = None) -> SimpleNamespace:
        if size is None:
            raise AssertionError("backend scan calls must pass an explicit size")
        observed_sizes.append(size)
        if page == 1:
            return SimpleNamespace(
                items=[secret_z],
                total_pages=2,
                max_size=size + 100,
            )
        if page == 2 and size == observed_sizes[0]:
            return SimpleNamespace(items=[secret_a], total_pages=2, max_size=size + 100)
        return SimpleNamespace(items=[], total_pages=2, max_size=size + 100)

    fake_client = Mock()
    fake_client.list_secrets.side_effect = list_secrets

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "list"])

    assert exc_info.value.code == 0
    assert len(observed_sizes) == 2
    assert observed_sizes[0] == observed_sizes[1]
    output = capsys.readouterr().out
    assert "alpha: secret-a (private)" in output
    assert "zeta: secret-z (public)" in output


def test_secrets_list_paginates_after_sorting(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`secrets list` should slice after deterministic name/id ordering."""
    secret_z = SimpleNamespace(name="zeta", id="secret-z", private=False)
    secret_b = SimpleNamespace(name="beta", id="secret-b", private=False)
    secret_a = SimpleNamespace(name="alpha", id="secret-a", private=True)
    fake_client = Mock()
    fake_client.list_secrets.return_value = SimpleNamespace(
        items=[secret_z, secret_b, secret_a],
        total_pages=1,
        max_size=3,
    )

    with (
        patch("kitaru.cli.Client", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["secrets", "list", "--page", "1", "--size", "2"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "alpha: secret-a (private)" in output
    assert "beta: secret-b (public)" in output
    assert "zeta" not in output
    assert "Page 1 (size 2, showing 2 of 3)" in output


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


def test_project_list_renders_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project list` should render visible projects and active marker."""
    with (
        patch("kitaru.cli.list_projects") as mock_list_projects,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_projects.return_value = [
            _project_stub(name="dev", is_active=False),
            _project_stub(name="prod", is_active=True),
        ]
        app(["project", "list"])

    assert exc_info.value.code == 0
    mock_list_projects.assert_called_once_with(page=1, size=20)
    output = capsys.readouterr().out
    assert "Kitaru projects" in output
    assert "dev: project-dev-id" in output
    assert "prod: project-prod-id (active)" in output


def test_project_current_renders_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project current` should show active project details."""
    with (
        patch("kitaru.cli.current_project") as mock_current_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_current_project.return_value = _project_stub(name="prod", is_active=True)
        app(["project", "current"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru project" in output
    assert "Project: prod" in output
    assert "Project ID: project-prod-id" in output


def test_project_show_renders_snapshot(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project show` should render Kitaru project details."""
    with (
        patch("kitaru.cli.get_project") as mock_get_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_get_project.return_value = _project_stub(
            name="prod",
            display_name="Production",
            description="Production project",
            is_active=True,
        )
        app(["project", "show", "prod"])

    assert exc_info.value.code == 0
    mock_get_project.assert_called_once_with("prod")
    output = capsys.readouterr().out
    assert "Kitaru project" in output
    assert "Name: prod" in output
    assert "Display name: Production" in output
    assert "Description: Production project" in output
    assert "Active: yes" in output


def test_project_use_delegates_to_config(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project use` should activate and report the selected project."""
    with (
        patch("kitaru.cli.use_project") as mock_use_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_use_project.return_value = _project_stub(name="prod", is_active=True)
        app(["project", "use", "prod"])

    assert exc_info.value.code == 0
    mock_use_project.assert_called_once_with("prod")
    output = capsys.readouterr().out
    assert "Activated project: prod" in output
    assert "Project ID: project-prod-id" in output


def test_project_create_reports_auto_activation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project create` should create and activate by default."""
    with (
        patch("kitaru.cli.create_project") as mock_create_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_project.return_value = _project_create_result_stub()
        app(["project", "create", "staging"])

    assert exc_info.value.code == 0
    mock_create_project.assert_called_once_with("staging", activate=True)
    output = capsys.readouterr().out
    assert "Created project: staging" in output
    assert "Activated project: prod → staging" in output


def test_project_create_no_activate_skips_activation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project create --no-activate` should not activate the new project."""
    with (
        patch("kitaru.cli.create_project") as mock_create_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_project.return_value = _project_create_result_stub(
            activated=False,
        )
        app(["project", "create", "staging", "--no-activate"])

    assert exc_info.value.code == 0
    mock_create_project.assert_called_once_with("staging", activate=False)
    output = capsys.readouterr().out
    assert "Created project: staging" in output
    assert "Activated project" not in output


def test_project_create_warns_when_env_overrides_activation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project create` should not claim effective activation if env wins."""
    with (
        patch("kitaru.cli.create_project") as mock_create_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_project.return_value = _project_create_result_stub(
            activated=True,
            is_active=False,
        )
        app(["project", "create", "staging"])

    assert exc_info.value.code == 0
    mock_create_project.assert_called_once_with("staging", activate=True)
    streams = capsys.readouterr()
    assert "Created project: staging" in streams.out
    assert "Activated project" not in streams.out
    assert "Project activation is still overridden" in streams.err
    assert "KITARU_PROJECT" in streams.err


def test_project_delete_requires_yes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project delete` should fail before backend calls without --yes."""
    with (
        patch("kitaru.cli.delete_project") as mock_delete_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["project", "delete", "staging"])

    assert exc_info.value.code == 1
    mock_delete_project.assert_not_called()
    assert "Refusing to delete project without --yes." in capsys.readouterr().err


def test_project_delete_reports_success(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru project delete --yes` should delete and report the project."""
    with (
        patch("kitaru.cli.delete_project") as mock_delete_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_delete_project.return_value = _project_delete_result_stub()
        app(["project", "delete", "staging", "--yes"])

    assert exc_info.value.code == 0
    mock_delete_project.assert_called_once_with("staging")
    assert "Deleted project: staging" in capsys.readouterr().out


def test_project_list_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru project list --output json` should emit serialized projects."""
    with (
        patch("kitaru.cli.list_projects") as mock_list_projects,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_projects.return_value = [
            _project_stub(name="dev", is_active=False),
            _project_stub(name="prod", is_active=True),
        ]
        app(["project", "list", "--output", "json"])

    assert exc_info.value.code == 0
    mock_list_projects.assert_called_once_with(page=1, size=20)
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "project.list",
        "items": [
            {
                "id": "project-dev-id",
                "name": "dev",
                "display_name": None,
                "description": None,
                "is_active": False,
            },
            {
                "id": "project-prod-id",
                "name": "prod",
                "display_name": None,
                "description": None,
                "is_active": True,
            },
        ],
        "count": 2,
    }


def test_project_current_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru project current --output json` should emit one project item."""
    with (
        patch("kitaru.cli.current_project") as mock_current_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_current_project.return_value = _project_stub(name="prod", is_active=True)
        app(["project", "current", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "project.current",
        "item": {
            "id": "project-prod-id",
            "name": "prod",
            "display_name": None,
            "description": None,
            "is_active": True,
        },
    }


def test_project_create_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """`kitaru project create --output json` should include activation metadata."""
    with (
        patch("kitaru.cli.create_project") as mock_create_project,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_create_project.return_value = _project_create_result_stub()
        app(["project", "create", "staging", "--output", "json"])

    assert exc_info.value.code == 0
    mock_create_project.assert_called_once_with("staging", activate=True)
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "project.create",
        "item": {
            "id": "project-staging-id",
            "name": "staging",
            "display_name": None,
            "description": None,
            "is_active": True,
            "previous_active_project": "prod",
            "activated": True,
        },
    }


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
    assert "Sandbox: my-k8s-sandbox (local)" in output
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
                {
                    "role": "sandbox",
                    "name": "my-k8s-sandbox",
                    "backend": "local",
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
        sandbox_flavor="local",
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
        sandbox_flavor="local",
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
                "dev (sandbox)",
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


def test_stack_create_kubernetes_passes_explicit_sandbox() -> None:
    """`--sandbox` should pass an explicit sandbox flavor for remote stacks."""
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
                "sandbox": "local",
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
                "--sandbox",
                "local",
            ]
        )

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.kwargs["sandbox_flavor"] == "local"


def test_stack_create_rejects_blank_sandbox(capsys: pytest.CaptureFixture[str]) -> None:
    """Blank sandbox flavor strings should fail validation."""
    with pytest.raises(SystemExit) as exc_info:
        app(["stack", "create", "dev", "--sandbox", "   "])

    assert exc_info.value.code == 1
    assert "--sandbox cannot be empty." in capsys.readouterr().err


def test_stack_create_remote_rejects_sandbox_extra_without_sandbox(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Remote stack sandbox overrides require an explicit sandbox flavor."""
    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
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
                "sandbox.forward_env=false",
            ]
        )

    assert exc_info.value.code == 1
    stderr = capsys.readouterr().err
    assert "--extra sandbox.*" in stderr
    assert "--sandbox" in stderr
    mock_create_stack.assert_not_called()


def test_stack_create_file_rejects_sandbox_extra_without_sandbox(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """YAML sandbox overrides for remote stacks require top-level sandbox."""
    stack_file = _write_stack_create_file(
        tmp_path,
        """
name: yaml-vertex
type: vertex
artifact_store: gs://bucket/kitaru
container_registry: us-central1-docker.pkg.dev/demo/repo
region: us-central1
extra:
  sandbox:
    forward_env: false
""".strip(),
    )

    with (
        patch("kitaru.cli._create_stack_operation") as mock_create_stack,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["stack", "create", "--file", str(stack_file)])

    assert exc_info.value.code == 1
    stderr = capsys.readouterr().err
    assert "--extra sandbox.*" in stderr
    assert "--sandbox" in stderr
    mock_create_stack.assert_not_called()


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
        "sandbox": {},
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
sandbox: local
async: true
extra:
  orchestrator:
    pipeline_root: gs://bucket/root
  container_registry:
    default_repository: from-yaml
  sandbox:
    forward_env: false
    sandbox_environment:
      FROM_YAML: yes
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
                "--extra",
                "sandbox.sandbox_environment.FROM_CLI=enabled",
            ]
        )

    assert exc_info.value.code == 0
    assert mock_create_stack.call_args.kwargs["sandbox_flavor"] == "local"
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
        "sandbox": {
            "forward_env": False,
            "sandbox_environment": {"FROM_YAML": True, "FROM_CLI": "enabled"},
        },
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
        sandbox_flavor="local",
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


def test_model_list_json_paginates_without_metadata(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Paginated list JSON should keep the existing envelope shape."""
    with (
        patch("kitaru.cli.list_model_aliases") as mock_list_models,
        pytest.raises(SystemExit) as exc_info,
    ):
        mock_list_models.return_value = [
            SimpleNamespace(
                alias="fast",
                model="openai/gpt-4o-mini",
                secret=None,
                is_default=True,
            ),
            SimpleNamespace(
                alias="smart",
                model="anthropic/claude-sonnet-4-20250514",
                secret=None,
                is_default=False,
            ),
        ]
        app(["model", "list", "--page", "2", "--size", "1", "--output", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"command", "items", "count"}
    assert payload["command"] == "model.list"
    assert payload["count"] == 1
    assert payload["items"][0]["alias"] == "smart"


def test_secrets_set_json_output_accepts_output_before_assignments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru secrets set --output json` should still parse assignment tokens."""
    fake_client = Mock()
    fake_client.create_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=False,
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
    assert payload["item"]["visibility"] == "public"


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
        patch("kitaru._inspection_runtime.GlobalConfiguration", return_value=fake_gc),
        patch(
            "kitaru._inspection_runtime.connected_to_local_server", return_value=False
        ),
        patch(
            "kitaru._inspection_runtime.get_local_server",
            return_value=fake_local_server,
        ),
        patch(
            "kitaru._inspection_runtime.Client",
            side_effect=AssertionError("Client should not be queried"),
        ),
    ):
        snapshot = _build_runtime_snapshot()

    assert snapshot.warning is not None
    assert "stopped local server" in snapshot.warning


def test_describe_local_server_handles_missing_local_backend() -> None:
    """Local server rendering should not crash when local server extras are missing."""
    with patch(
        "kitaru._inspection_runtime.get_local_server",
        side_effect=ImportError("missing"),
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
                return_value=("not running", False, False),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "global", "--yes"])
        assert exc_info.value.code == 1
        output = capsys.readouterr().err
        assert "3 aliases" in output
        assert "--force" in output

    def test_non_interactive_without_yes_exits_nonzero(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """Non-interactive clean without --yes should fail, not silently abort."""
        config_root = tmp_path / "config"
        config_root.mkdir()

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
                return_value=("not running", False, False),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "global"])  # no --yes, non-interactive stdin
        assert exc_info.value.code == 1
        output = capsys.readouterr().err
        assert "--yes" in output

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
                return_value=("not running", False, False),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "global", "--dry-run"])
        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        assert "Backup" in output or "backup" in output

    def test_dry_run_surfaces_local_server_inspection_failure(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """Dry-run should warn when local-server state cannot be inspected."""
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
                "zenml.utils.server_utils.get_local_server",
                side_effect=RuntimeError("registry unreadable"),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "global", "--dry-run", "-o", "json"])

        assert exc_info.value.code == 0
        payload = json.loads(capsys.readouterr().out)
        item = payload["item"]
        assert "inspection failed: registry unreadable" in item["local_server_status"]
        assert any(
            "Could not inspect the registered local server" in warning
            for warning in item["warnings"]
        )


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
                return_value=("not running", False, False),
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

    def test_global_cleanup_warns_when_local_server_cannot_stop_safely(
        self, tmp_path: Path
    ) -> None:
        """Cleanup should warn and continue instead of killing a stored PID."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )
        from kitaru._local_server import LocalServerCleanupResult

        config_root = tmp_path / "config"
        config_root.mkdir()

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root=str(config_root),
            model_registry_alias_count=0,
            local_server_would_stop=True,
        )

        with (
            patch("kitaru._cleanup._reset_global_config"),
            patch(
                "kitaru._local_server.stop_registered_local_server_for_cleanup",
                return_value=LocalServerCleanupResult(
                    stopped=False,
                    url="http://localhost:8383",
                    force_killed_pid=None,
                ),
            ),
        ):
            result = execute_cleanup_plan(plan, yes=True, force=False)

        assert not result.aborted
        assert result.local_server_stopped is False
        assert result.local_server_force_killed_pid is None
        assert str(config_root) in result.deleted_paths
        assert any(
            "did not kill the stored PID" in warning
            and "PID-only evidence can be stale" in warning
            for warning in result.warnings
        )

    def test_global_cleanup_inspection_failure_warning_uses_explicit_state(
        self, tmp_path: Path
    ) -> None:
        """Inspection-failure warnings should not depend on status text."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )
        from kitaru._local_server import LocalServerCleanupResult

        config_root = tmp_path / "config"
        config_root.mkdir()

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root=str(config_root),
            model_registry_alias_count=0,
            local_server_status="local server state could not be read",
            local_server_would_stop=True,
            local_server_inspection_failed=True,
        )

        with (
            patch("kitaru._cleanup._reset_global_config"),
            patch(
                "kitaru._local_server.stop_registered_local_server_for_cleanup",
                return_value=LocalServerCleanupResult(
                    stopped=True,
                    url=None,
                    force_killed_pid=None,
                ),
            ) as mock_stop,
        ):
            result = execute_cleanup_plan(plan, yes=True, force=False)

        mock_stop.assert_called_once_with(timeout=10)
        assert result.local_server_inspection_failed is True
        assert any(
            "Could not inspect the registered local server" in warning
            and "local server state could not be read" in warning
            for warning in result.warnings
        )

    def test_preview_warning_does_not_parse_unknown_status_prefix(self) -> None:
        """Display text alone should not create an inspection-failure warning."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            build_cleanup_preview_result,
        )

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            local_server_status="unknown (inspection failed: display text only)",
            local_server_inspection_failed=False,
        )

        result = build_cleanup_preview_result(plan)

        assert result.local_server_inspection_failed is False
        assert result.warnings == ()

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
        """OSError during project deletion should produce a warning, not a crash."""
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

    def test_user_declines_confirmation_aborts(self, tmp_path: Path) -> None:
        """When prompt_confirm returns False, result should be aborted."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("data\n")

        plan = CleanupPlan(
            scope=CleanScope.PROJECT,
            repo_root=str(tmp_path),
            project_config_path=str(project_dir),
        )

        result = execute_cleanup_plan(
            plan,
            yes=False,
            force=False,
            prompt_confirm=lambda _: False,
        )

        assert result.aborted
        assert project_dir.exists(), "Directory must NOT be deleted when user declines"

    def test_non_interactive_without_yes_raises(self, tmp_path: Path) -> None:
        """Missing prompt_confirm with yes=False should raise."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        plan = CleanupPlan(
            scope=CleanScope.PROJECT,
            repo_root=str(tmp_path),
            project_config_path=str(tmp_path / ".kitaru"),
        )

        with pytest.raises(KitaruUsageError, match="Non-interactive"):
            execute_cleanup_plan(plan, yes=False, force=False, prompt_confirm=None)

    def test_backup_failure_blocks_cleanup(self, tmp_path: Path) -> None:
        """When backup fails, cleanup should abort and leave config intact."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "keep-me.txt").write_text("important")

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root=str(config_root),
            backup_path=str(tmp_path / "backups" / "backup.db"),
            model_registry_alias_count=0,
        )

        with (
            patch(
                "kitaru._cleanup._create_backup",
                side_effect=OSError("disk full"),
            ),
            pytest.raises(KitaruUsageError, match="backup"),
        ):
            execute_cleanup_plan(plan, yes=True, force=False)

        assert config_root.exists(), "Config dir must survive when backup fails"

    def test_global_deletion_failure_raises_error(self, tmp_path: Path) -> None:
        """OSError during global config deletion should raise, not warn."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        config_root = tmp_path / "config"
        config_root.mkdir()

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root=str(config_root),
            model_registry_alias_count=0,
        )

        with (
            patch(
                "kitaru._cleanup._delete_directory",
                side_effect=OSError("Permission denied"),
            ),
            pytest.raises(KitaruUsageError, match="delete config directory"),
        ):
            execute_cleanup_plan(plan, yes=True, force=False)

    def test_path_safety_refuses_home_directory(self, tmp_path: Path) -> None:
        """Cleanup should refuse to delete the home directory."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            execute_cleanup_plan,
        )

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root=str(Path.home()),
            model_registry_alias_count=0,
        )

        with pytest.raises(KitaruUsageError, match="home directory"):
            execute_cleanup_plan(plan, yes=True, force=True)


class TestStopRegisteredLocalServerForCleanup:
    """Tests for stop_registered_local_server_for_cleanup."""

    def test_graceful_stop_succeeds(self) -> None:
        """When graceful shutdown works, should return stopped=True."""
        from kitaru._local_server import stop_registered_local_server_for_cleanup

        mock_deployer = MagicMock()
        mock_server = SimpleNamespace(
            status=SimpleNamespace(url="http://localhost:8383"),
            config=None,
        )

        with patch(
            "kitaru._local_server._load_local_server_runtime",
            return_value=(mock_deployer, None, None, lambda: mock_server),
        ):
            result = stop_registered_local_server_for_cleanup(timeout=5)

        assert result.stopped is True
        assert result.force_killed_pid is None
        mock_deployer.return_value.remove_server.assert_called_once_with(timeout=5)

    def test_graceful_fails_does_not_kill_stored_pid(self) -> None:
        """When graceful shutdown fails, cleanup does not kill by PID only."""
        from kitaru._local_server import stop_registered_local_server_for_cleanup

        mock_deployer_cls = MagicMock()
        mock_deployer_cls.return_value.remove_server.side_effect = RuntimeError("fail")
        mock_server = SimpleNamespace(
            status=SimpleNamespace(url="http://localhost:8383", pid=42),
            config=None,
        )

        with (
            patch(
                "kitaru._local_server._load_local_server_runtime",
                return_value=(mock_deployer_cls, None, None, lambda: mock_server),
            ),
            patch("os.kill") as mock_kill,
        ):
            result = stop_registered_local_server_for_cleanup(timeout=5)

        assert result.stopped is False
        assert result.url == "http://localhost:8383"
        assert result.force_killed_pid is None
        mock_deployer_cls.return_value.remove_server.assert_called_once_with(timeout=5)
        mock_kill.assert_not_called()

    def test_import_error_returns_not_stopped(self) -> None:
        """ImportError from loading runtime should return stopped=False."""
        from kitaru._local_server import stop_registered_local_server_for_cleanup

        with patch(
            "kitaru._local_server._load_local_server_runtime",
            side_effect=ImportError("no module"),
        ):
            result = stop_registered_local_server_for_cleanup(timeout=5)

        assert result.stopped is False

    def test_no_server_returns_not_stopped(self) -> None:
        """When no server is registered, should return stopped=False."""
        from kitaru._local_server import stop_registered_local_server_for_cleanup

        mock_deployer = MagicMock()

        with patch(
            "kitaru._local_server._load_local_server_runtime",
            return_value=(mock_deployer, None, None, lambda: None),
        ):
            result = stop_registered_local_server_for_cleanup(timeout=5)

        assert result.stopped is False

    def test_inspection_error_returns_not_stopped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Failures while inspecting the registered server should not raise."""
        from kitaru._local_server import stop_registered_local_server_for_cleanup

        def raise_inspection_error() -> None:
            raise RuntimeError("corrupt daemon registration")

        mock_deployer = MagicMock()

        with patch(
            "kitaru._local_server._load_local_server_runtime",
            return_value=(mock_deployer, None, None, raise_inspection_error),
        ):
            result = stop_registered_local_server_for_cleanup(timeout=5)

        assert result.stopped is False
        assert result.url is None
        assert "Could not inspect registered local server" in caplog.text
        mock_deployer.return_value.remove_server.assert_not_called()


class TestPathSafety:
    """Tests for _validate_deletion_target."""

    def test_refuses_root(self) -> None:
        from kitaru._cleanup import _validate_deletion_target

        with pytest.raises(KitaruUsageError, match="filesystem root"):
            _validate_deletion_target(Path("/"))

    def test_refuses_home(self) -> None:
        from kitaru._cleanup import _validate_deletion_target

        with pytest.raises(KitaruUsageError, match="home directory"):
            _validate_deletion_target(Path.home())

    def test_refuses_cwd(self) -> None:
        from kitaru._cleanup import _validate_deletion_target

        with pytest.raises(KitaruUsageError, match="working directory"):
            _validate_deletion_target(Path.cwd())

    def test_accepts_subdirectory(self, tmp_path: Path) -> None:
        from kitaru._cleanup import _validate_deletion_target

        subdir = tmp_path / "safe-to-delete"
        subdir.mkdir()
        _validate_deletion_target(subdir)  # should not raise


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


def _snapshot_with_active_context_provenance() -> RuntimeSnapshot:
    return RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/config",
        active_stack="prod",
        active_project="default",
        active_stack_provenance=ActiveConfigSelectionProvenance(
            resource="active_stack",
            effective_source="repo-local config",
            effective_source_detail="/work/repo/.kitaru/config.yaml",
            effective_id="repo-stack-id",
            resolved_id="resolved-stack-id",
            resolved_name="prod",
            environment_variable="ZENML_ACTIVE_STACK_ID",
            environment_id=None,
            repository_root="/work/repo",
            repository_config_path="/work/repo/.kitaru/config.yaml",
            repository_id="repo-stack-id",
            global_config_path="/tmp/config/config.yaml",
            global_id="global-stack-id",
            notes=[
                "KITARU_STACK is an execution default and does not set "
                "ZenML's active stack."
            ],
        ),
        active_project_provenance=ActiveConfigSelectionProvenance(
            resource="active_project",
            effective_source="environment",
            effective_source_detail="KITARU_PROJECT -> ZENML_ACTIVE_PROJECT_ID",
            effective_id="env-project-id",
            resolved_id="resolved-project-id",
            resolved_name="default",
            environment_variable="KITARU_PROJECT -> ZENML_ACTIVE_PROJECT_ID",
            environment_id="env-project-id",
            repository_root="/work/repo",
            repository_config_path="/work/repo/.kitaru/config.yaml",
            repository_id="repo-project-id",
            global_config_path="/tmp/config/config.yaml",
            global_id="global-project-id",
        ),
    )


def test_info_default_does_not_render_active_context_provenance(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Default `kitaru info` should not show the verbose provenance section."""
    snapshot = _snapshot_with_active_context_provenance()

    with (
        patch(
            "kitaru.cli._build_runtime_snapshot", return_value=snapshot
        ) as mock_build,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    mock_build.assert_called_once_with(
        include_packages=False,
        package_names=None,
        include_environment_type=False,
        include_provenance_details=False,
    )
    output = capsys.readouterr().out
    assert "Active context provenance" not in output
    assert "repo-stack-id" not in output
    assert "global-project-id" not in output


def test_status_does_not_render_active_context_provenance(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru status` should stay compact even if a snapshot has provenance."""
    snapshot = _snapshot_with_active_context_provenance()

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Kitaru status" in output
    assert "Active context provenance" not in output
    assert "repo-stack-id" not in output


def test_status_renders_active_context_fallback_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru status` should surface fallback warnings in normal output."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
        active_stack="default",
        warning=(
            "Kitaru detected that the saved active context changed while loading.\n"
            "Configured active stack from repo-local config points to ID "
            "'stale-stack-id', but Kitaru loaded default (default-stack-id)."
        ),
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Warning" in output
    assert "saved active context changed" in output
    assert "stale-stack-id" in output


def test_info_default_renders_active_context_fallback_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Default `kitaru info` should show warnings without verbose provenance."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.3.0",
        connection="local database",
        connection_target="sqlite:///...",
        config_directory="/tmp/config",
        active_stack="default",
        warning="Kitaru detected that the saved active context changed.",
    )

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "saved active context changed" in output
    assert "Active context provenance" not in output


def test_info_all_renders_active_context_provenance(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info --all` should show active stack/project provenance."""
    snapshot = _snapshot_with_active_context_provenance()

    with (
        patch(
            "kitaru.cli._build_runtime_snapshot", return_value=snapshot
        ) as mock_build,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "--all"])

    assert exc_info.value.code == 0
    mock_build.assert_called_once_with(
        include_packages=True,
        package_names=None,
        include_environment_type=True,
        include_provenance_details=True,
    )
    output = capsys.readouterr().out
    assert "Active context provenance" in output
    assert "Active stack source: repo-local config" in output
    assert "Active stack configured ID: repo-stack-id" in output
    assert "Active stack resolved: prod (resolved-stack-id)" in output
    assert "Active project source: environment" in output
    assert "Active project configured ID: env-project-id" in output
    assert "KITARU_STACK is an execution default" in output


def test_info_all_json_includes_active_context_provenance(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info --all -o json` should serialize structured provenance."""
    snapshot = _snapshot_with_active_context_provenance()

    with (
        patch(
            "kitaru.cli._build_runtime_snapshot", return_value=snapshot
        ) as mock_build,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "--all", "-o", "json"])

    assert exc_info.value.code == 0
    mock_build.assert_called_once_with(
        include_packages=True,
        package_names=None,
        include_environment_type=True,
        include_provenance_details=True,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "info"
    item = payload["item"]
    assert item["active_stack_provenance"]["effective_id"] == "repo-stack-id"
    assert item["active_stack_provenance"]["resolved_name"] == "prod"
    assert item["active_project_provenance"]["environment_id"] == "env-project-id"


def test_status_json_hides_active_context_provenance_by_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru status -o json` should hide verbose provenance by default."""
    snapshot = _snapshot_with_active_context_provenance()

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["status", "-o", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "status"
    item = payload["item"]
    assert item["active_stack_provenance"] is None
    assert item["active_project_provenance"] is None


def test_info_json_hides_active_context_provenance_by_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info -o json` should hide verbose provenance unless --all."""
    snapshot = _snapshot_with_active_context_provenance()

    with (
        patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "-o", "json"])

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "info"
    item = payload["item"]
    assert item["active_stack_provenance"] is None
    assert item["active_project_provenance"] is None


def test_info_all_file_export_includes_active_context_provenance(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru info --all --file` should export structured provenance."""
    snapshot = _snapshot_with_active_context_provenance()
    export_path = tmp_path / "debug.json"

    with (
        patch(
            "kitaru.cli._build_runtime_snapshot", return_value=snapshot
        ) as mock_build,
        pytest.raises(SystemExit) as exc_info,
    ):
        app(["info", "--all", "--file", str(export_path)])

    assert exc_info.value.code == 0
    mock_build.assert_called_once_with(
        include_packages=True,
        package_names=None,
        include_environment_type=True,
        include_provenance_details=True,
    )
    assert str(export_path) in capsys.readouterr().out
    payload = json.loads(export_path.read_text())
    assert payload["active_stack_provenance"]["effective_id"] == "repo-stack-id"
    assert payload["active_project_provenance"]["resolved_id"] == "resolved-project-id"


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
    assert data["active_stack_provenance"] is None
    assert data["active_project_provenance"] is None


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


# ---------------------------------------------------------------------------
# Analytics: info / status / clean feature-level tracking
# ---------------------------------------------------------------------------


class TestStatusAnalytics:
    """Verify status command fires STATUS_VIEWED."""

    def test_status_fires_status_viewed(self) -> None:
        """kitaru status should emit STATUS_VIEWED."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.3.0",
            connection="local database",
            connection_target="sqlite:///...",
            config_directory="/tmp/config",
        )
        with (
            patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["status"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(AnalyticsEvent.STATUS_VIEWED)


class TestInfoAnalytics:
    """Verify info command fires INFO_VIEWED with correct metadata."""

    def test_info_fires_info_viewed(self) -> None:
        """kitaru info (no flags) should emit INFO_VIEWED."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.3.0",
            connection="local database",
            connection_target="sqlite:///...",
            config_directory="/tmp/config",
        )
        with (
            patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["info"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.INFO_VIEWED,
            {"all": False, "packages_requested": False},
        )

    def test_info_all_tracks_correctly(self) -> None:
        """kitaru info --all should include all=True in metadata."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.3.0",
            connection="local database",
            connection_target="sqlite:///...",
            config_directory="/tmp/config",
        )
        with (
            patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["info", "--all"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.INFO_VIEWED,
            {"all": True, "packages_requested": True},
        )

    def test_info_file_export_fires_both_events(self, tmp_path: Path) -> None:
        """kitaru info --file should fire INFO_VIEWED and INFO_EXPORTED."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.3.0",
            connection="local database",
            connection_target="sqlite:///...",
            config_directory="/tmp/config",
        )
        export_path = tmp_path / "debug.json"
        with (
            patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["info", "--file", str(export_path)])

        assert exc_info.value.code == 0
        assert track_mock.call_count == 2

        viewed_call = track_mock.call_args_list[0]
        assert viewed_call.args[0] == AnalyticsEvent.INFO_VIEWED

        exported_call = track_mock.call_args_list[1]
        assert exported_call.args[0] == AnalyticsEvent.INFO_EXPORTED
        assert exported_call.args[1] == {"format": "json"}

    def test_info_metadata_contains_no_paths(self) -> None:
        """INFO_VIEWED metadata must not leak file paths or user data."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.3.0",
            connection="local database",
            connection_target="sqlite:///...",
            config_directory="/tmp/config",
        )
        with (
            patch("kitaru.cli._build_runtime_snapshot", return_value=snapshot),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["info", "-p", "numpy"])

        assert exc_info.value.code == 0
        metadata = track_mock.call_args.args[1]
        for value in metadata.values():
            assert not isinstance(value, str) or "/" not in value


class TestCleanAnalytics:
    """Verify clean command analytics coverage."""

    def test_dry_run_fires_clean_completed_with_dry_run_true(
        self,
        tmp_path: Path,
    ) -> None:
        """clean project --dry-run should fire CLEAN_COMPLETED(dry_run=True)."""
        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=tmp_path,
            ),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "project", "--dry-run"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.CLEAN_COMPLETED,
            {"scope": "project", "dry_run": True},
        )

    def test_actual_clean_fires_clean_completed_with_dry_run_false(
        self,
        tmp_path: Path,
    ) -> None:
        """clean project --yes should fire CLEAN_COMPLETED(dry_run=False)."""
        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=tmp_path,
            ),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "project", "--yes"])

        assert exc_info.value.code == 0
        track_mock.assert_called_once_with(
            AnalyticsEvent.CLEAN_COMPLETED,
            {"scope": "project", "dry_run": False},
        )

    def test_clean_metadata_contains_no_user_data(
        self,
        tmp_path: Path,
    ) -> None:
        """CLEAN_COMPLETED metadata should only have scope and dry_run."""
        project_dir = tmp_path / ".kitaru"
        project_dir.mkdir()
        (project_dir / "config.yaml").write_text("active_stack: default\n")

        with (
            patch(
                "kitaru._cleanup._resolve_repo_root",
                return_value=tmp_path,
            ),
            patch("kitaru.analytics.track") as track_mock,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["clean", "project", "--dry-run"])

        assert exc_info.value.code == 0
        metadata = track_mock.call_args.args[1]
        assert set(metadata.keys()) == {"scope", "dry_run"}
        assert metadata["scope"] in ("project", "global", "all")


# ---------------------------------------------------------------------------
# Analytics CLI
# ---------------------------------------------------------------------------


class TestAnalyticsStatus:
    """Tests for kitaru analytics status."""

    @pytest.mark.parametrize(
        ("opt_in", "expected_label"),
        [(True, "enabled"), (False, "disabled")],
    )
    def test_status_text_output(
        self,
        capsys: pytest.CaptureFixture[str],
        opt_in: bool,
        expected_label: str,
    ) -> None:
        with (
            patch(
                "kitaru._cli._analytics._get_analytics_opt_in",
                return_value=opt_in,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "status"])

        assert exc_info.value.code == 0
        output = capsys.readouterr().out
        assert expected_label in output

    def test_status_json_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with (
            patch(
                "kitaru._cli._analytics._get_analytics_opt_in",
                return_value=False,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "status", "-o", "json"])

        assert exc_info.value.code == 0
        envelope = json.loads(capsys.readouterr().out)
        assert envelope["command"] == "analytics.status"
        assert envelope["item"]["analytics_opt_in"] is False


class TestAnalyticsOptIn:
    """Tests for kitaru analytics opt-in."""

    def test_opt_in_text_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with (
            patch(
                "kitaru._cli._analytics._set_analytics_opt_in",
            ) as mock_set,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "opt-in"])

        assert exc_info.value.code == 0
        mock_set.assert_called_once_with(True)
        output = capsys.readouterr().out
        assert "Analytics enabled." in output

    def test_opt_in_json_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with (
            patch("kitaru._cli._analytics._set_analytics_opt_in"),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "opt-in", "-o", "json"])

        assert exc_info.value.code == 0
        envelope = json.loads(capsys.readouterr().out)
        assert envelope["command"] == "analytics.opt-in"
        assert envelope["item"]["analytics_opt_in"] is True


class TestAnalyticsOptOut:
    """Tests for kitaru analytics opt-out."""

    def test_opt_out_text_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with (
            patch(
                "kitaru._cli._analytics._set_analytics_opt_in",
            ) as mock_set,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "opt-out"])

        assert exc_info.value.code == 0
        mock_set.assert_called_once_with(False)
        output = capsys.readouterr().out
        assert "Analytics disabled." in output
        assert "MCP" in output

    def test_opt_out_json_output(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with (
            patch("kitaru._cli._analytics._set_analytics_opt_in"),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "opt-out", "-o", "json"])

        assert exc_info.value.code == 0
        envelope = json.loads(capsys.readouterr().out)
        assert envelope["command"] == "analytics.opt-out"
        assert envelope["item"]["analytics_opt_in"] is False

    def test_opt_out_surfaces_config_error(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with (
            patch(
                "kitaru._cli._analytics._set_analytics_opt_in",
                side_effect=RuntimeError("Config file corrupted"),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(["analytics", "opt-out"])

        assert exc_info.value.code == 1
        output = capsys.readouterr().err
        assert "Config file corrupted" in output
