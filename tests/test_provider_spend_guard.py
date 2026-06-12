"""Tests for pytest provider-spend guard helpers."""

from __future__ import annotations

from tests.conftest import _provider_name_for_host, _validate_live_marker_contract


def _assert_marker_contract_accepts(*items: tuple[str, set[str]]) -> None:
    assert _validate_live_marker_contract(items) == []


def _assert_marker_contract_rejects(
    *items: tuple[str, set[str]],
    match: str,
) -> None:
    errors = _validate_live_marker_contract(items)
    assert any(match in error for error in errors)


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
