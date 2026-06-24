"""Tests for Kitaru UI deep-link base URL resolution."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kitaru._config._env import KITARU_UI_URL_ENV
from kitaru._ui_urls import resolve_ui_base_url


def test_resolve_ui_base_url_prefers_kitaru_ui_url_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        KITARU_UI_URL_ENV,
        "https://preview.demo.kitaru.zenml.io/",
    )
    with patch(
        "kitaru.config.resolve_connection_config",
        return_value=SimpleNamespace(
            server_url="https://161e5333-zenml.staging.cloudinfra.zenml.io"
        ),
    ):
        assert (
            resolve_ui_base_url()
            == "https://preview.demo.kitaru.zenml.io"
        )


def test_resolve_ui_base_url_falls_back_to_connected_server_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KITARU_UI_URL_ENV, raising=False)
    with patch(
        "kitaru.config.resolve_connection_config",
        return_value=SimpleNamespace(
            server_url="https://161e5333-zenml.staging.cloudinfra.zenml.io/"
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
    fake_client._client.return_value.zen_store.url = (
        "https://demo.kitaru.zenml.io/"
    )

    with patch("kitaru.config.resolve_connection_config", side_effect=RuntimeError):
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
