"""Tests for Kitaru's public secret-read API."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest
from zenml.exceptions import EntityExistsError

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruBackendError, KitaruRuntimeError, KitaruUsageError
from kitaru.secrets import (
    Secret,
    SecretSummary,
    _read_secret_values,
    create_secret,
    delete_secret,
    get_secret,
    list_secrets,
)


def test_list_secrets_scans_one_backend_page() -> None:
    """SDK listing should request the first backend page at the fixed size."""
    client = Mock()
    client.list_secrets.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                name="openai-creds",
                id="secret-id",
                private=False,
                values={"OPENAI_API_KEY": object()},
                has_missing_values=False,
            )
        ],
        total_pages=1,
    )

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        summaries = list_secrets()

    client.list_secrets.assert_called_once_with(page=1, size=50)
    assert summaries == [
        SecretSummary(
            name="openai-creds",
            id="secret-id",
            private=False,
            keys=["OPENAI_API_KEY"],
        )
    ]


def test_list_secrets_scans_all_backend_pages_at_fixed_size() -> None:
    """SDK listing should fetch every reported page with the same scan size."""
    client = Mock()
    client.list_secrets.side_effect = [
        SimpleNamespace(
            items=[
                SimpleNamespace(
                    name="charlie",
                    id="3",
                    private=False,
                    values={},
                )
            ],
            total_pages=3,
        ),
        SimpleNamespace(
            items=[
                SimpleNamespace(
                    name="alpha",
                    id="1",
                    private=False,
                    values={},
                )
            ]
        ),
        SimpleNamespace(
            items=[
                SimpleNamespace(
                    name="bravo",
                    id="2",
                    private=False,
                    values={},
                )
            ]
        ),
    ]

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        summaries = list_secrets()

    assert client.list_secrets.call_args_list == [
        call(page=1, size=50),
        call(page=2, size=50),
        call(page=3, size=50),
    ]
    assert [summary.name for summary in summaries] == ["alpha", "bravo", "charlie"]


def test_list_secrets_returns_metadata_without_raw_values() -> None:
    """SDK listing should normalize IDs and expose key names, not values."""
    client = Mock()
    client.list_secrets.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                name="provider-creds",
                id=123,
                private=True,
                values={"Z_TOKEN": object(), "A_KEY": object()},
                secret_values={"Z_TOKEN": "secret-z", "A_KEY": "secret-a"},
                has_missing_values=True,
            )
        ],
        total_pages=1,
    )

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        summary = list_secrets()[0]

    assert summary == SecretSummary(
        name="provider-creds",
        id="123",
        private=True,
        keys=["A_KEY", "Z_TOKEN"],
        has_missing_values=True,
    )
    payload = summary.model_dump()
    assert "values" not in payload
    assert "secret_values" not in payload
    assert "secret-a" not in str(payload)
    assert "secret-z" not in str(payload)


def test_list_secrets_orders_by_case_insensitive_name_then_id() -> None:
    """SDK listing should return deterministic global ordering."""
    client = Mock()
    client.list_secrets.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(name="bravo", id="3", private=False, values={}),
            SimpleNamespace(name="alpha", id="2", private=False, values={}),
            SimpleNamespace(name="Alpha", id="1", private=False, values={}),
        ],
        total_pages=1,
    )

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        summaries = list_secrets()

    assert [(summary.name, summary.id) for summary in summaries] == [
        ("Alpha", "1"),
        ("alpha", "2"),
        ("bravo", "3"),
    ]


def test_list_secrets_returns_empty_list_for_empty_backend() -> None:
    """An accessible backend with no secrets should return an empty list."""
    client = Mock()
    client.list_secrets.return_value = SimpleNamespace(items=[], total_pages=1)

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        assert list_secrets() == []


def test_list_secrets_maps_client_initialization_failure_to_backend_error() -> None:
    """Client construction failures should become Kitaru backend errors."""
    with (
        patch("kitaru.secrets._ZenMLClient", side_effect=RuntimeError("offline")),
        pytest.raises(KitaruBackendError, match="Failed to list secrets: offline"),
    ):
        list_secrets()


def test_list_secrets_discards_partial_results_after_request_failure() -> None:
    """A later page failure should raise instead of returning earlier items."""
    client = Mock()
    client.list_secrets.side_effect = [
        SimpleNamespace(
            items=[
                SimpleNamespace(
                    name="first-page-secret",
                    id="1",
                    private=False,
                    values={},
                )
            ],
            total_pages=2,
        ),
        RuntimeError("offline"),
    ]

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(KitaruBackendError, match="Failed to list secrets: offline"),
    ):
        list_secrets()

    assert client.list_secrets.call_args_list == [
        call(page=1, size=50),
        call(page=2, size=50),
    ]


def test_list_secrets_maps_malformed_response_to_backend_error() -> None:
    """Malformed backend items should not escape as public summaries."""
    client = Mock()
    client.list_secrets.return_value = SimpleNamespace(
        items=[SimpleNamespace(id="missing-name", private=False, values={})],
        total_pages=1,
    )

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(
            KitaruBackendError,
            match=(
                "Failed to list secrets: Backend returned a secret "
                "without a readable name"
            ),
        ),
    ):
        list_secrets()


def test_get_secret_fetches_exact_secret_and_normalizes_values() -> None:
    """Public secret reads should exact-match and return a Kitaru model."""
    client = Mock()
    client.get_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id=123,
        private=True,
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
        private=True,
    )
    assert secret.get("OPENAI_API_KEY") == "sk-123"
    assert secret.get("MISSING") is None
    assert secret.get("MISSING", "fallback") == "fallback"
    track_mock.assert_called_once_with(AnalyticsEvent.SECRET_READ, {"key_count": 2})


def test_create_secret_creates_public_secret_by_default() -> None:
    """SDK secret creation should default to public metadata-only writes."""
    client = Mock()
    client.create_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id=123,
        private=False,
        values={"OPENAI_API_KEY": object(), "COUNT": object()},
        has_missing_values=False,
    )

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        patch("kitaru.secrets.track") as track_mock,
    ):
        summary = create_secret(
            " openai-creds ",
            {"OPENAI_API_KEY": "sk-123", "COUNT": 3},
        )

    client.create_secret.assert_called_once_with(
        name="openai-creds",
        values={"OPENAI_API_KEY": "sk-123", "COUNT": "3"},
        private=False,
    )
    assert summary == SecretSummary(
        name="openai-creds",
        id="123",
        private=False,
        keys=["COUNT", "OPENAI_API_KEY"],
        has_missing_values=False,
    )
    track_mock.assert_called_once_with(
        AnalyticsEvent.SECRET_UPSERTED,
        {"operation": "created", "key_count": 2},
    )


def test_create_secret_forwards_private_flag() -> None:
    """Callers can opt into private secret creation."""
    client = Mock()
    client.create_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=True,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
    )

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        summary = create_secret(
            "openai-creds",
            {"OPENAI_API_KEY": "sk-123"},
            private=True,
        )

    client.create_secret.assert_called_once_with(
        name="openai-creds",
        values={"OPENAI_API_KEY": "sk-123"},
        private=True,
    )
    assert summary.private is True


@pytest.mark.parametrize(
    ("name", "values", "message"),
    [
        ("   ", {"OPENAI_API_KEY": "sk-123"}, "Secret name or ID cannot be empty"),
        ("openai-creds", {}, "Provide at least one secret value"),
        (
            "openai-creds",
            {"1_BAD": "sk-123"},
            "Invalid secret key `1_BAD`",
        ),
        (
            "openai-creds",
            {"OPENAI_API_KEY": ""},
            "Secret value for key `OPENAI_API_KEY` cannot be empty",
        ),
        (
            "openai-creds",
            {"OPENAI_API_KEY": None},
            "Secret value for key `OPENAI_API_KEY` cannot be None",
        ),
        (
            "openai-creds",
            {"TOKEN": "first", " TOKEN ": "second"},
            "Duplicate secret key `TOKEN`",
        ),
    ],
)
def test_create_secret_rejects_invalid_inputs(
    name: str,
    values: dict[str, object],
    message: str,
) -> None:
    """Invalid write inputs should fail before contacting the backend."""
    with pytest.raises(KitaruUsageError, match=message):
        create_secret(name, values)


def test_create_secret_maps_existing_secret_to_runtime_error() -> None:
    """Existing backend secrets should become a Kitaru runtime error."""
    client = Mock()
    client.create_secret.side_effect = EntityExistsError("already exists")

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(KitaruRuntimeError, match="Secret `openai-creds` already exists"),
    ):
        create_secret("openai-creds", {"OPENAI_API_KEY": "sk-123"})


def test_create_secret_maps_backend_failure_to_backend_error() -> None:
    """Unexpected create failures should not leak raw client errors."""
    client = Mock()
    client.create_secret.side_effect = RuntimeError("offline")

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(
            KitaruBackendError, match="Failed to create secret `openai-creds`"
        ),
    ):
        create_secret("openai-creds", {"OPENAI_API_KEY": "sk-123"})


def test_delete_secret_resolves_exact_secret_and_deletes_by_id() -> None:
    """SDK deletion should exact-resolve first and delete the resolved ID."""
    client = Mock()
    client.get_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id=123,
        private=False,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
    )

    with patch("kitaru.secrets._ZenMLClient", return_value=client):
        summary = delete_secret(" openai-creds ")

    client.get_secret.assert_called_once_with(
        name_id_or_prefix="openai-creds",
        allow_partial_name_match=False,
        allow_partial_id_match=False,
    )
    client.delete_secret.assert_called_once_with(name_id_or_prefix="123")
    assert summary == SecretSummary(
        name="openai-creds",
        id="123",
        private=False,
        keys=["OPENAI_API_KEY"],
    )


def test_delete_secret_maps_client_initialization_failure_to_backend_error() -> None:
    """Client construction failures should become Kitaru backend errors."""
    with (
        patch("kitaru.secrets._ZenMLClient", side_effect=RuntimeError("offline")),
        pytest.raises(
            KitaruBackendError, match="Failed to delete secret `openai-creds`"
        ),
    ):
        delete_secret("openai-creds")


def test_delete_secret_maps_backend_failure_to_backend_error() -> None:
    """Delete failures should become Kitaru backend errors."""
    client = Mock()
    client.get_secret.return_value = SimpleNamespace(
        name="openai-creds",
        id="secret-id",
        private=False,
        values={"OPENAI_API_KEY": object()},
        has_missing_values=False,
    )
    client.delete_secret.side_effect = RuntimeError("offline")

    with (
        patch("kitaru.secrets._ZenMLClient", return_value=client),
        pytest.raises(
            KitaruBackendError, match="Failed to delete secret `openai-creds`"
        ),
    ):
        delete_secret("openai-creds")


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
