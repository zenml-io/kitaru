"""Tests for Kitaru UI deep-link URL resolution and builders."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kitaru._config._env import KITARU_UI_URL_ENV
from kitaru._ui_urls import (
    build_compare_url_from_context,
    resolve_ui_base_url,
    resolve_ui_url_context,
)

_CLOUDINFRA_BACKEND_URL = "https://67e44b28-zenml.staging.cloudinfra.zenml.io"


def _pro_server_info(
    *,
    pro_dashboard_url: str | None = "https://staging.cloud.zenml.io",
    dashboard_url: str | None = "https://staging.cloud.zenml.io/workspaces/kitaru-dev",
    pro_workspace_name: str | None = "kitaru-dev",
    pro_workspace_id: str | None = "59f7a3b3-f50f-405c-b4fe-ea341cd0ffed",
    metadata: dict[str, str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_type="cloud",
        pro_dashboard_url=pro_dashboard_url,
        dashboard_url=dashboard_url,
        pro_workspace_name=pro_workspace_name,
        pro_workspace_id=pro_workspace_id,
        metadata=metadata or {},
    )


def test_resolve_ui_base_url_prefers_kitaru_ui_url_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        KITARU_UI_URL_ENV,
        "https://preview.demo.kitaru.zenml.io/",
    )
    with (
        patch(
            "kitaru._ui_urls._server_info_from_client",
            side_effect=AssertionError("base URL resolution should stay cheap"),
        ),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(
                server_url="https://161e5333-zenml.staging.cloudinfra.zenml.io"
            ),
        ),
    ):
        assert resolve_ui_base_url() == "https://preview.demo.kitaru.zenml.io"


def test_resolve_ui_url_context_uses_pro_dashboard_origin_and_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "https://staging.cloud.zenml.io"
    assert context.route_kind == "pro"
    assert context.workspace == "kitaru-dev"
    assert context.explicit_override is False


def test_resolve_ui_url_context_uses_pro_dashboard_not_connection_server_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with (
        patch(
            "kitaru._ui_urls._server_info_from_client",
            return_value=_pro_server_info(),
        ),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(
                server_url="https://67e44b28-zenml.staging.cloudinfra.zenml.io"
            ),
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "https://staging.cloud.zenml.io"
    assert context.route_kind == "pro"
    assert context.workspace == "kitaru-dev"


def test_resolve_ui_url_context_uses_dashboard_url_origin_when_pro_origin_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(
            pro_dashboard_url=None,
            dashboard_url="https://staging.cloud.zenml.io/workspaces/kitaru-dev",
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "https://staging.cloud.zenml.io"
    assert context.route_kind == "pro"
    assert context.workspace == "kitaru-dev"


def test_resolve_ui_url_context_uses_metadata_workspace_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(
            pro_workspace_name=None,
            metadata={"workspace_name": "metadata-workspace"},
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.route_kind == "pro"
    assert context.workspace == "metadata-workspace"


def test_resolve_ui_url_context_uses_workspace_id_when_name_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(
            pro_workspace_name=None,
            pro_workspace_id="59f7a3b3-f50f-405c-b4fe-ea341cd0ffed",
            metadata={},
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.route_kind == "pro"
    assert context.workspace == "59f7a3b3-f50f-405c-b4fe-ea341cd0ffed"


def test_resolve_ui_url_context_returns_none_for_cloud_with_missing_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with (
        patch(
            "kitaru._ui_urls._server_info_from_client",
            return_value=_pro_server_info(
                pro_workspace_name=None,
                pro_workspace_id=None,
                metadata={},
            ),
        ),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(
                server_url="https://67e44b28-zenml.staging.cloudinfra.zenml.io"
            ),
        ),
    ):
        context = resolve_ui_url_context()

    assert context is None


def test_resolve_ui_url_context_returns_none_for_cloudinfra_connection_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with (
        patch("kitaru._ui_urls._server_info_from_client", return_value=None),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(server_url=_CLOUDINFRA_BACKEND_URL),
        ),
    ):
        context = resolve_ui_url_context()

    assert context is None


def test_resolve_ui_url_context_returns_none_when_cloudinfra_metadata_lookup_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)
    fake_client = MagicMock()
    fake_client._client.return_value.zen_store.get_store_info.side_effect = RuntimeError

    with patch(
        "kitaru.config.resolve_connection_config",
        return_value=SimpleNamespace(server_url=f"{_CLOUDINFRA_BACKEND_URL}/api/v1"),
    ):
        context = resolve_ui_url_context(fake_client)

    assert context is None


def test_resolve_ui_url_context_returns_none_for_cloudinfra_client_store_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)
    fake_client = MagicMock()
    fake_client._client.return_value.zen_store.get_store_info.return_value = None
    fake_client._client.return_value.zen_store.url = f"{_CLOUDINFRA_BACKEND_URL}/"

    with patch("kitaru.config.resolve_connection_config", side_effect=RuntimeError):
        context = resolve_ui_url_context(fake_client)

    assert context is None


def test_resolve_ui_url_context_preserves_local_connection_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)

    with (
        patch("kitaru._ui_urls._server_info_from_client", return_value=None),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(server_url="http://127.0.0.1:8383/"),
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "http://127.0.0.1:8383"
    assert context.route_kind == "legacy"
    assert context.source == "connection_config"


def test_resolve_ui_url_context_preserves_custom_legacy_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(KITARU_UI_URL_ENV, "https://preview.demo.kitaru.zenml.io/")

    with (
        patch("kitaru._ui_urls._server_info_from_client", return_value=None),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(server_url=_CLOUDINFRA_BACKEND_URL),
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "https://preview.demo.kitaru.zenml.io"
    assert context.route_kind == "legacy"
    assert context.source == "env"
    assert context.explicit_override is True


def test_resolve_ui_url_context_returns_none_for_cloudinfra_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(KITARU_UI_URL_ENV, _CLOUDINFRA_BACKEND_URL)

    with patch("kitaru._ui_urls._server_info_from_client", return_value=None):
        context = resolve_ui_url_context()

    assert context is None


def test_resolve_ui_url_context_skips_incomplete_cloud_despite_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        KITARU_UI_URL_ENV,
        "https://preview.demo.kitaru.zenml.io",
    )

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(
            pro_workspace_name=None,
            pro_workspace_id=None,
            metadata={},
        ),
    ):
        context = resolve_ui_url_context()

    assert context is None


def test_resolve_ui_url_context_uses_override_as_pro_origin_when_cloud_workspace_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        KITARU_UI_URL_ENV,
        "https://preview.demo.kitaru.zenml.io",
    )

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(
            pro_dashboard_url=None,
            dashboard_url=None,
            pro_workspace_name="kitaru-dev",
        ),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "https://preview.demo.kitaru.zenml.io"
    assert context.route_kind == "pro"
    assert context.workspace == "kitaru-dev"
    assert context.explicit_override is True


def test_kitaru_ui_url_override_preserves_pro_route_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        KITARU_UI_URL_ENV,
        "https://preview.demo.kitaru.zenml.io",
    )

    with patch(
        "kitaru._ui_urls._server_info_from_client",
        return_value=_pro_server_info(),
    ):
        context = resolve_ui_url_context()

    assert context is not None
    assert context.base_url == "https://preview.demo.kitaru.zenml.io"
    assert context.route_kind == "pro"
    assert context.workspace == "kitaru-dev"
    assert context.explicit_override is True

    assert build_compare_url_from_context(
        context,
        flow_id="flow-1",
        exec_ids=["kr-original", "kr-replay"],
        project_name_or_id="default",
    ) == (
        "https://preview.demo.kitaru.zenml.io/kitaru-workspaces/kitaru-dev"
        "/projects/default/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay"
    )


def test_resolve_ui_base_url_falls_back_to_connected_server_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)
    with (
        patch("kitaru._ui_urls._server_info_from_client", return_value=None),
        patch(
            "kitaru.config.resolve_connection_config",
            return_value=SimpleNamespace(
                server_url="https://161e5333-zenml.staging.cloudinfra.zenml.io/"
            ),
        ),
    ):
        assert (
            resolve_ui_base_url()
            == "https://161e5333-zenml.staging.cloudinfra.zenml.io"
        )


def test_resolve_ui_base_url_uses_client_store_when_connection_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)
    fake_client = MagicMock()
    fake_client._client.return_value.zen_store.url = "https://demo.kitaru.zenml.io/"

    with (
        patch("kitaru._ui_urls._server_info_from_client", return_value=None),
        patch("kitaru.config.resolve_connection_config", side_effect=RuntimeError),
    ):
        assert resolve_ui_base_url(fake_client) == "https://demo.kitaru.zenml.io"


def test_compare_urls_use_kitaru_ui_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        KITARU_UI_URL_ENV,
        "https://preview.demo.kitaru.zenml.io",
    )
    original = MagicMock()
    original.flow_id = "flow-1"
    original.metadata = {}

    fake_client = MagicMock()
    fake_client.executions.get.return_value = original
    fake_client._client.return_value.zen_store.url = (
        "https://161e5333-zenml.staging.cloudinfra.zenml.io"
    )

    with patch("kitaru._ui_urls._server_info_from_client", return_value=None):
        from kitaru.diff import compare_urls_for_replay

        urls = compare_urls_for_replay(
            fake_client,
            original_exec_id="kr-original",
            replay_exec_id="kr-replay",
        )

    assert urls == [
        "https://preview.demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay"
    ]
