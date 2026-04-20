"""Tests for Kitaru's public secret-read API."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruBackendError, KitaruRuntimeError, KitaruUsageError
from kitaru.secrets import Secret, _read_secret_values, get_secret


def test_get_secret_fetches_exact_secret_and_normalizes_values() -> None:
    """Public secret reads should exact-match and return a Kitaru model."""
    client = Mock()
    client.get_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id=123,
        secret_values={
            "OPENAI_API_KEY": "sk-123",
            "COUNT": 3,
            "": "skip-empty-key",
            "NONE": None,
        },
    )

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        patch("kitaru.secrets.track") as track_mock,
    ):
        secret = get_secret(" openai-creds ")

    client.get_secret.assert_called_once_with(
        name_id_or_prefix="openai-creds",
        allow_partial_name_match=False,
        allow_partial_id_match=False,
    )
    assert secret == Secret(
        name="openai-creds",
        id="123",
        values={"OPENAI_API_KEY": "sk-123", "COUNT": "3"},
    )
    assert secret.get("OPENAI_API_KEY") == "sk-123"
    assert secret.get("MISSING") is None
    assert secret.get("MISSING", "fallback") == "fallback"
    track_mock.assert_called_once_with(AnalyticsEvent.SECRET_READ, {"key_count": 2})


def test_get_secret_rejects_empty_name() -> None:
    """Empty names should fail before Kitaru contacts the backend."""
    with pytest.raises(KitaruUsageError, match="Secret name or ID cannot be empty"):
        get_secret("   ")


def test_get_secret_maps_missing_secret_to_runtime_error() -> None:
    """Backend not-found errors should become Kitaru runtime errors."""
    client = Mock()
    client.get_secret.side_effect = KeyError("missing")

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(KitaruRuntimeError, match="Secret `github-creds` was not found"),
    ):
        get_secret("github-creds")


def test_get_secret_maps_backend_failure_to_backend_error() -> None:
    """Unexpected backend failures should not leak raw client errors."""
    client = Mock()
    client.get_secret.side_effect = RuntimeError("offline")

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(KitaruBackendError, match="Failed to load secret `github-creds`"),
    ):
        get_secret("github-creds")


def test_get_secret_rejects_unreadable_values() -> None:
    """A malformed backend response should not become a public Secret."""
    client = Mock()
    client.get_secret.return_value = SimpleNamespace(
        name="github-creds",
        id="secret-id",
        secret_values=None,
    )

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(
            KitaruRuntimeError,
            match="Secret `github-creds` does not contain readable key/value pairs",
        ),
    ):
        get_secret("github-creds")


def test_read_secret_values_rejects_empty_normalized_values() -> None:
    """Internal credential reads should still require at least one usable value."""
    client = Mock()
    client.get_secret.return_value = SimpleNamespace(
        name="empty-creds",
        id="secret-id",
        secret_values={"": "skip", "NONE": None},
    )

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(
            KitaruRuntimeError,
            match="Secret `empty-creds` does not contain readable key/value pairs",
        ),
    ):
        _read_secret_values("empty-creds")
