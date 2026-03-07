"""Tests for the Kitaru package public API surface."""

from __future__ import annotations

from unittest.mock import patch

import pytest

import kitaru


def test_package_imports() -> None:
    assert kitaru.__name__ == "kitaru"


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

    def test_kitaru_client_exists(self) -> None:
        assert hasattr(kitaru, "KitaruClient")

    def test_all_exports_match(self) -> None:
        expected = {
            "FlowHandle",
            "KitaruClient",
            "checkpoint",
            "configure",
            "connect",
            "flow",
            "llm",
            "load",
            "log",
            "save",
            "wait",
        }
        assert set(kitaru.__all__) == expected


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
        with pytest.raises(ValueError, match="Invalid Kitaru server URL"):
            kitaru.connect("example.com")


class TestPlaceholderBehavior:
    """Verify implemented/scaffolded primitive behavior in the current phase."""

    def test_flow_returns_callable_with_start_and_deploy(self) -> None:
        wrapped = kitaru.flow(lambda: None)
        assert callable(wrapped)
        assert hasattr(wrapped, "start")
        assert hasattr(wrapped, "deploy")

    def test_checkpoint_returns_callable_with_submit(self) -> None:
        with patch("kitaru.checkpoint.step") as step_factory:
            zenml_step = object()
            step_factory.return_value = lambda func: zenml_step
            wrapped = kitaru.checkpoint(lambda: None)

        assert callable(wrapped)
        assert hasattr(wrapped, "submit")

    def test_wait_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="wait"):
            kitaru.wait()

    def test_llm_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="llm"):
            kitaru.llm("hello")

    def test_save_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="save"):
            kitaru.save("name", "value")

    def test_load_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="load"):
            kitaru.load("exec-123", "name")

    def test_log_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="log"):
            kitaru.log(cost=0.01)

    def test_configure_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="configure"):
            kitaru.configure(cache=False)

    def test_client_raises(self) -> None:
        with pytest.raises(NotImplementedError, match="KitaruClient"):
            kitaru.KitaruClient()
