"""Tests for pytest provider-spend guard helpers."""

from __future__ import annotations

from tests.conftest import _provider_name_for_host


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
