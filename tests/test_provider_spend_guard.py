"""Tests for pytest provider-spend guard helpers."""

from __future__ import annotations

import threading
from typing import cast

import pytest

from tests.conftest import (
    _guard_provider_call_if_active,
    _missing_live_provider_auth_messages,
    _provider_name_for_host,
    _set_provider_call_guard_state,
    _validate_live_marker_contract,
)


class _FakePytestItem:
    def __init__(self, marker_names: set[str]) -> None:
        self._marker_names = marker_names

    def get_closest_marker(self, marker_name: str) -> object | None:
        if marker_name in self._marker_names:
            return object()
        return None


def _fake_pytest_item(marker_names: set[str]) -> pytest.Item:
    return cast(pytest.Item, _FakePytestItem(marker_names))


def _assert_marker_contract_accepts(*items: tuple[str, set[str]]) -> None:
    assert _validate_live_marker_contract(items) == []


def _assert_marker_contract_rejects(
    *items: tuple[str, set[str]],
    match: str,
) -> None:
    errors = _validate_live_marker_contract(items)
    assert any(match in error for error in errors)


def test_provider_spend_guard_fixture_activates_for_non_live_tests() -> None:
    with pytest.raises(AssertionError, match="Blocked OpenAI provider call"):
        _guard_provider_call_if_active("OpenAI")


def test_provider_spend_guard_inactive_state_allows_provider_calls() -> None:
    _set_provider_call_guard_state(active=False, nodeid="tests/live/test_example.py")
    _guard_provider_call_if_active("OpenAI")


def test_provider_spend_guard_state_is_visible_to_background_threads() -> None:
    errors: list[BaseException] = []

    def attempt_provider_call() -> None:
        try:
            _guard_provider_call_if_active("Anthropic")
        except BaseException as exc:
            errors.append(exc)

    _set_provider_call_guard_state(active=True, nodeid="tests/test_example.py")
    thread = threading.Thread(target=attempt_provider_call)
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], AssertionError)
    assert "Blocked Anthropic provider call" in str(errors[0])


def test_live_openai_auth_message_reports_missing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    missing = _missing_live_provider_auth_messages(_fake_pytest_item({"live_openai"}))

    assert missing == ["OPENAI_API_KEY"]


def test_live_openai_auth_message_accepts_present_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    missing = _missing_live_provider_auth_messages(_fake_pytest_item({"live_openai"}))

    assert missing == []


def test_provider_spend_guard_blocks_httpx_provider_request_before_transport() -> None:
    httpx = pytest.importorskip("httpx")
    requests_seen: list[object] = []

    def handler(request: object) -> object:
        requests_seen.append(request)
        return httpx.Response(200)

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(AssertionError, match="Blocked OpenAI provider call"),
    ):
        client.get("https://api.openai.com/v1/models")

    assert requests_seen == []


def test_provider_spend_guard_allows_httpx_localhost_request() -> None:
    httpx = pytest.importorskip("httpx")
    requests_seen: list[object] = []

    def handler(request: object) -> object:
        requests_seen.append(request)
        return httpx.Response(200)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        response = client.get("http://127.0.0.1:9999/health")

    assert response.status_code == 200
    assert len(requests_seen) == 1


def test_provider_spend_guard_matches_vertex_global_and_regional_hosts() -> None:
    """Vertex regional endpoints use REGION-aiplatform.googleapis.com."""
    assert _provider_name_for_host("aiplatform.googleapis.com") == "Gemini/Google GenAI"
    assert (
        _provider_name_for_host("us-central1-aiplatform.googleapis.com")
        == "Gemini/Google GenAI"
    )
    assert (
        _provider_name_for_host("europe-west4-aiplatform.googleapis.com.")
        == "Gemini/Google GenAI"
    )


def test_provider_spend_guard_does_not_match_unrelated_aiplatform_like_hosts() -> None:
    assert _provider_name_for_host("not-aiplatform.example.com") is None
    assert _provider_name_for_host("aiplatform.googleapis.com.example.com") is None


def test_provider_spend_guard_matches_azure_openai_resource_hosts() -> None:
    assert _provider_name_for_host("api.openai.azure.com") == "OpenAI"
    assert _provider_name_for_host("aoairesource.openai.azure.com") == "OpenAI"
    assert _provider_name_for_host("AOAIRESOURCE.openai.azure.com.") == "OpenAI"


def test_provider_spend_guard_does_not_match_azure_openai_lookalikes() -> None:
    assert _provider_name_for_host("openai.azure.com") is None
    assert _provider_name_for_host("nested.aoairesource.openai.azure.com") is None
    assert _provider_name_for_host("aoairesource.openai.azure.com.example.com") is None
    assert _provider_name_for_host("aoairesource-openai.azure.com") is None


def test_provider_marker_without_live_llm_fails_collection() -> None:
    _assert_marker_contract_rejects(
        ("tests/live/test_example.py::test_openai", {"live_openai"}),
        match="must also be marked",
    )


def test_live_llm_without_provider_marker_fails_collection() -> None:
    _assert_marker_contract_rejects(
        ("tests/live/test_example.py::test_provider", {"live_llm"}),
        match="must also declare",
    )


def test_live_llm_with_provider_marker_passes_collection() -> None:
    _assert_marker_contract_accepts(
        ("tests/live/test_example.py::test_openai", {"live_llm", "live_openai"})
    )
