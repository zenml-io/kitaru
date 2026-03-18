"""Tests for the Kitaru package public API surface."""

from __future__ import annotations

import importlib
from unittest.mock import Mock, patch

import pytest

import kitaru


def test_package_imports() -> None:
    assert kitaru.__name__ == "kitaru"


def test_package_reload_applies_env_translations() -> None:
    """Reloading the package should re-run env translation at import time."""
    with patch("kitaru._env.apply_env_translations") as apply_translations:
        importlib.reload(kitaru)

    apply_translations.assert_called_once_with()


def test_package_import_does_not_require_project_until_first_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Import should tolerate missing project so benign commands still work."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://server.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")

    importlib.reload(kitaru)

    assert kitaru.__name__ == "kitaru"


def test_package_import_rejects_partial_connection_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Import should fail fast on server-only or token-only env config."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://server.example.com")

    with pytest.raises(RuntimeError, match="KITARU_AUTH_TOKEN"):
        importlib.reload(kitaru)

    monkeypatch.delenv("KITARU_SERVER_URL", raising=False)
    importlib.reload(kitaru)


class TestPublicExports:
    """Verify all public SDK primitives are importable."""

    def test_flow_exists(self) -> None:
        assert hasattr(kitaru, "flow")

    def test_checkpoint_exists(self) -> None:
        assert hasattr(kitaru, "checkpoint")

    def test_wait_exists(self) -> None:
        assert hasattr(kitaru, "wait")

    def test_llm_exists(self) -> None:
        assert hasattr(kitaru, "llm")

    def test_save_exists(self) -> None:
        assert hasattr(kitaru, "save")

    def test_load_exists(self) -> None:
        assert hasattr(kitaru, "load")

    def test_log_exists(self) -> None:
        assert hasattr(kitaru, "log")

    def test_configure_exists(self) -> None:
        assert hasattr(kitaru, "configure")

    def test_connect_exists(self) -> None:
        assert hasattr(kitaru, "connect")

    def test_list_stacks_exists(self) -> None:
        assert hasattr(kitaru, "list_stacks")

    def test_current_stack_exists(self) -> None:
        assert hasattr(kitaru, "current_stack")

    def test_use_stack_exists(self) -> None:
        assert hasattr(kitaru, "use_stack")

    def test_create_stack_exists(self) -> None:
        assert hasattr(kitaru, "create_stack")

    def test_delete_stack_exists(self) -> None:
        assert hasattr(kitaru, "delete_stack")

    def test_kitaru_client_exists(self) -> None:
        assert hasattr(kitaru, "KitaruClient")

    def test_all_exports_match(self) -> None:
        expected = {
            "FailureOrigin",
            "FlowHandle",
            "ImageSettings",
            "KitaruBackendError",
            "KitaruClient",
            "KitaruConfig",
            "KitaruContextError",
            "KitaruDivergenceError",
            "KitaruError",
            "KitaruExecutionError",
            "KitaruFeatureNotAvailableError",
            "KitaruLogRetrievalError",
            "KitaruRuntimeError",
            "KitaruStateError",
            "KitaruUsageError",
            "KitaruUserCodeError",
            "KitaruWaitValidationError",
            "StackInfo",
            "checkpoint",
            "configure",
            "connect",
            "create_stack",
            "current_stack",
            "delete_stack",
            "flow",
            "list_stacks",
            "llm",
            "load",
            "log",
            "save",
            "use_stack",
            "wait",
        }
        assert set(kitaru.__all__) == expected


class TestDirectImportStyle:
    """Verify canonical import style works correctly."""

    def test_direct_decorator_imports(self) -> None:
        from kitaru import checkpoint, flow

        assert flow is kitaru.flow
        assert checkpoint is kitaru.checkpoint


class TestImplementedConnectionPrimitive:
    """Verify the early Phase 2 connection primitive works as intended."""

    def test_connect_delegates_to_login_router(self) -> None:
        with patch("kitaru.config._login_to_server_target") as mock_login:
            kitaru.connect(
                "https://example.com/",
                api_key="secret-key",
                refresh=True,
                project="demo-project",
                no_verify_ssl=True,
            )

        mock_login.assert_called_once_with(
            "https://example.com",
            api_key="secret-key",
            refresh=True,
            project="demo-project",
            verify_ssl=False,
            cloud_api_url=None,
        )

    def test_connect_routes_pro_urls_to_managed_login(self) -> None:
        with (
            patch(
                "kitaru.config._zenml_is_pro_server",
                return_value=(True, "https://cloudapi.example.com"),
            ),
            patch("kitaru.config._zenml_connect_to_pro_server") as mock_pro_login,
            patch("kitaru.config._zenml_connect_to_server") as mock_direct_login,
        ):
            kitaru.connect("https://example.com/")

        mock_direct_login.assert_not_called()
        mock_pro_login.assert_called_once_with(
            pro_server="https://example.com",
            api_key=None,
            refresh=False,
            pro_api_url="https://cloudapi.example.com",
            verify_ssl=True,
            project=None,
        )

    def test_login_to_server_routes_workspace_names_to_managed_login(self) -> None:
        with (
            patch("kitaru.config._zenml_connect_to_pro_server") as mock_pro_login,
            patch("kitaru.config._zenml_connect_to_server") as mock_direct_login,
        ):
            from kitaru.config import login_to_server

            login_to_server(
                "pause-resume",
                project="kitaru",
                cloud_api_url="https://staging.cloudapi.zenml.io/",
            )

        mock_direct_login.assert_not_called()
        mock_pro_login.assert_called_once_with(
            pro_server="pause-resume",
            api_key=None,
            refresh=False,
            pro_api_url="https://staging.cloudapi.zenml.io/",
            verify_ssl=True,
            project="kitaru",
        )

    def test_login_to_server_uses_managed_login_for_url_with_cloud_api(self) -> None:
        with (
            patch("kitaru.config._zenml_connect_to_pro_server") as mock_pro_login,
            patch("kitaru.config._zenml_connect_to_server") as mock_direct_login,
        ):
            from kitaru.config import login_to_server

            login_to_server(
                "https://example.com/",
                cloud_api_url="https://staging.cloudapi.zenml.io/",
            )

        mock_direct_login.assert_not_called()
        mock_pro_login.assert_called_once_with(
            pro_server="https://example.com",
            api_key=None,
            refresh=False,
            pro_api_url="https://staging.cloudapi.zenml.io/",
            verify_ssl=True,
            project=None,
        )

    def test_connect_rejects_invalid_urls(self) -> None:
        with pytest.raises(kitaru.KitaruUsageError, match="Invalid Kitaru server URL"):
            kitaru.connect("example.com")

    def test_current_stack_returns_stack_info(self) -> None:
        with patch("kitaru.config.Client") as client_cls:
            client_cls.return_value.active_stack_model.id = "stack-prod-id"
            client_cls.return_value.active_stack_model.name = "prod"

            stack = kitaru.current_stack()

        assert stack.name == "prod"
        assert stack.id == "stack-prod-id"
        assert stack.is_active is True


class TestPlaceholderBehavior:
    """Verify implemented/scaffolded primitive behavior in the current phase."""

    def test_flow_returns_wrapper_with_run_and_deploy(self) -> None:
        wrapped = kitaru.flow(lambda: None)
        assert hasattr(wrapped, "run")
        assert hasattr(wrapped, "deploy")
        assert not hasattr(wrapped, "start")

    def test_checkpoint_returns_callable_with_submit(self) -> None:
        with patch("kitaru.checkpoint.step") as step_factory:
            zenml_step = object()
            step_factory.return_value = lambda func: zenml_step
            wrapped = kitaru.checkpoint(lambda: None)

        assert callable(wrapped)
        assert hasattr(wrapped, "submit")

    def test_wait_requires_flow_context(self) -> None:
        with pytest.raises(kitaru.KitaruContextError, match=r"@flow"):
            kitaru.wait()

    def test_wait_rejects_checkpoint_context(self) -> None:
        from kitaru.runtime import _checkpoint_scope, _flow_scope

        with (
            _flow_scope(name="flow_a"),
            _checkpoint_scope(
                name="checkpoint_a",
                checkpoint_type=None,
            ),
            pytest.raises(
                kitaru.KitaruContextError,
                match=r"@checkpoint",
            ),
        ):
            kitaru.wait()

    def test_wait_delegates_to_zenml_wait(self) -> None:
        from kitaru.runtime import _flow_scope

        mock_zenml_wait = Mock(return_value=True)

        with (
            _flow_scope(name="flow_a"),
            patch(
                "kitaru.wait._resolve_zenml_wait",
                return_value=mock_zenml_wait,
            ),
        ):
            resolved = kitaru.wait(
                name="approve_deploy",
                question="Approve deploy?",
                metadata={"service": "api"},
            )

        assert resolved is True
        mock_zenml_wait.assert_called_once_with(
            schema=bool,
            question="Approve deploy?",
            timeout=600,
            metadata={"service": "api"},
            name="approve_deploy",
        )

    def test_llm_requires_flow_context(self) -> None:
        with pytest.raises(kitaru.KitaruContextError, match=r"inside a @flow"):
            kitaru.llm("hello")

    def test_save_requires_checkpoint_context(self) -> None:
        with pytest.raises(kitaru.KitaruContextError, match=r"inside a @checkpoint"):
            kitaru.save("name", "value")

    def test_load_requires_checkpoint_context(self) -> None:
        with pytest.raises(kitaru.KitaruContextError, match=r"inside a @checkpoint"):
            kitaru.load("exec-123", "name")

    def test_log_requires_flow_context(self) -> None:
        with pytest.raises(kitaru.KitaruContextError, match=r"inside a @flow"):
            kitaru.log(cost=0.01)

    def test_configure_sets_runtime_defaults(self) -> None:
        snapshot = kitaru.configure(cache=False, retries=2)
        assert snapshot.cache is False
        assert snapshot.retries == 2

    def test_configure_accepts_project_override(self) -> None:
        kitaru.configure(project="staging-project")
        from kitaru.config import resolve_connection_config

        resolved = resolve_connection_config()
        assert resolved.project == "staging-project"

    def test_client_exposes_namespaces(self) -> None:
        client = kitaru.KitaruClient()
        assert hasattr(client, "executions")
        assert hasattr(client, "artifacts")
