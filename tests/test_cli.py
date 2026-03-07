"""Tests for the kitaru CLI."""

from __future__ import annotations

from importlib.metadata import version as get_version
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest
from zenml.exceptions import EntityExistsError

from kitaru.cli import (
    RuntimeSnapshot,
    _build_runtime_snapshot,
    _describe_local_server,
    _logout_current_connection,
    _parse_secret_assignments,
    app,
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


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """--version prints the package version and exits."""
    with pytest.raises(SystemExit) as exc_info:
        app(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    expected_version = get_version("kitaru")
    assert expected_version in captured.out


def test_short_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """-V also prints the version."""
    with pytest.raises(SystemExit) as exc_info:
        app(["-V"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    expected_version = get_version("kitaru")
    assert expected_version in captured.out


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
        "login",
        "logout",
        "status",
        "info",
        "log-store",
        "stack",
        "secrets",
        "model",
    ):
        assert command in output


def test_no_args_shows_help(capsys: pytest.CaptureFixture[str]) -> None:
    """Invoking with no arguments shows help output."""
    with pytest.raises(SystemExit) as exc_info:
        app([])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "kitaru" in captured.out.lower()


def test_login_delegates_to_connect(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`kitaru login` passes CLI options through to the login helper."""
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
        cloud_api_url=None,
    )

    output = capsys.readouterr().out
    assert "Connected to Kitaru server: https://example.com" in output
    assert "Active project: demo-project" in output


def test_login_surfaces_validation_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid server URLs should exit cleanly with a helpful error."""
    with pytest.raises(SystemExit) as exc_info:
        app(["login", "example.com"])

    assert exc_info.value.code == 1
    assert "Invalid Kitaru server URL" in capsys.readouterr().err


def test_login_accepts_server_url_alias(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--server-url` should remain a supported spelling for login."""
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
                "--server-url",
                "https://example.com/",
                "--project",
                "demo-project",
            ]
        )

    assert exc_info.value.code == 0
    mock_login.assert_called_once_with(
        "https://example.com/",
        api_key=None,
        refresh=False,
        project="demo-project",
        no_verify_ssl=False,
        ssl_ca_cert=None,
        cloud_api_url=None,
    )
    output = capsys.readouterr().out
    assert "Connected to Kitaru server: https://example.com" in output


def test_login_accepts_cloud_api_url_alias(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--pro-api-url` should remain a supported compatibility alias."""
    with (
        patch("kitaru.cli.login_to_server") as mock_login,
        patch(
            "kitaru.cli._get_connected_server_url",
            return_value="https://staging.example.com",
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "login",
                "pause-resume",
                "--pro-api-url",
                "https://staging.cloudapi.zenml.io/",
                "--project",
                "kitaru",
            ]
        )

    assert exc_info.value.code == 0
    mock_login.assert_called_once_with(
        "pause-resume",
        api_key=None,
        refresh=False,
        project="kitaru",
        no_verify_ssl=False,
        ssl_ca_cert=None,
        cloud_api_url="https://staging.cloudapi.zenml.io/",
    )
    output = capsys.readouterr().out
    assert "Connected to Kitaru server: https://staging.example.com" in output


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


def test_logout_resets_remote_connection() -> None:
    """The logout helper should reset the active store and clear credentials."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="https://example.com/")
    fake_credentials_store = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch(
            "kitaru.cli.get_credentials_store",
            return_value=fake_credentials_store,
        ),
    ):
        message = _logout_current_connection()

    fake_gc.set_default_store.assert_called_once_with()
    fake_credentials_store.clear_credentials.assert_called_once_with(
        "https://example.com"
    )
    assert message == "Logged out from Kitaru server: https://example.com"


def test_logout_is_idempotent_on_local_store() -> None:
    """The logout helper should be a no-op when already on the local store."""
    fake_gc = Mock()
    fake_gc.uses_local_store = True

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
    ):
        message = _logout_current_connection()

    assert message == "Kitaru is already using its local default store."


def test_logout_clears_remote_store_when_local_fallback_is_missing() -> None:
    """Logout should still clear persisted remote state without local mode."""
    fake_gc = Mock()
    fake_gc.uses_local_store = False
    fake_gc.store_configuration = SimpleNamespace(url="http://127.0.0.1:8237")
    fake_gc.set_default_store.side_effect = ImportError("sqlalchemy missing")
    fake_credentials_store = Mock()

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch(
            "kitaru.cli.get_credentials_store",
            return_value=fake_credentials_store,
        ),
    ):
        message = _logout_current_connection()

    fake_gc._write_config.assert_called_once_with()
    fake_credentials_store.clear_credentials.assert_called_once_with(
        "http://127.0.0.1:8237"
    )
    assert "local fallback unavailable" in message


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
        active_project="demo",
        active_stack="prod",
        config_directory="/tmp/.zenml",
        local_stores_path="/tmp/.zenml/local_stores",
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
    assert "Local stores path: /tmp/.zenml/local_stores" in output


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
        active_project="demo",
        active_stack="prod",
        repository_root="/work/repo",
        server_version="0.94.0",
        server_database="sqlite",
        server_deployment_type="oss",
        config_directory="/tmp/.zenml",
        local_stores_path="/tmp/.zenml/local_stores",
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


def test_build_runtime_snapshot_handles_missing_local_store() -> None:
    """Status/info should degrade gracefully if local mode support is missing."""
    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=_BrokenGlobalConfig()),
        patch("kitaru.cli.get_local_server", side_effect=ImportError("missing")),
    ):
        snapshot = _build_runtime_snapshot()

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
    fake_gc.config_directory = "/tmp/.zenml"
    fake_gc.local_stores_path = "/tmp/.zenml/local_stores"
    fake_local_server = SimpleNamespace(
        config=SimpleNamespace(provider=SimpleNamespace(value="daemon")),
        status=SimpleNamespace(
            url=None,
            status_message="service daemon is not running",
        ),
    )

    with (
        patch("kitaru.cli.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.cli._connected_to_local_server", return_value=False),
        patch("kitaru.cli.get_local_server", return_value=fake_local_server),
        patch(
            "kitaru.cli.Client",
            side_effect=AssertionError("Client should not be queried"),
        ),
    ):
        snapshot = _build_runtime_snapshot()

    assert snapshot.warning is not None
    assert "stopped local server" in snapshot.warning


def test_describe_local_server_handles_missing_local_backend() -> None:
    """Local server rendering should not crash when local server extras are missing."""
    with patch("kitaru.cli.get_local_server", side_effect=ImportError("missing")):
        status = _describe_local_server()

    assert status == "unavailable (local runtime support not installed)"
