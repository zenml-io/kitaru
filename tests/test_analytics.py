"""Tests for the Kitaru analytics helpers."""

from __future__ import annotations

from unittest.mock import patch

from kitaru.analytics import AnalyticsEvent, set_source, track


def test_set_source_prefixes_kitaru_suffix() -> None:
    """Bare suffixes should be normalized to Kitaru source values."""
    with (
        patch("zenml.analytics.source_context") as source_context_mock,
        patch(
            "zenml.enums.SourceContextTypes",
            side_effect=lambda value: f"normalized:{value}",
        ) as source_types_mock,
    ):
        set_source("cli")

    source_types_mock.assert_called_once_with("kitaru-cli")
    source_context_mock.set.assert_called_once_with("normalized:kitaru-cli")


def test_set_source_accepts_full_kitaru_source() -> None:
    """Canonical Kitaru source values should pass through unchanged."""
    with (
        patch("zenml.analytics.source_context") as source_context_mock,
        patch(
            "zenml.enums.SourceContextTypes",
            side_effect=lambda value: f"normalized:{value}",
        ) as source_types_mock,
    ):
        set_source("kitaru-mcp")

    source_types_mock.assert_called_once_with("kitaru-mcp")
    source_context_mock.set.assert_called_once_with("normalized:kitaru-mcp")


def test_track_passes_metadata_through_unchanged() -> None:
    """track() should delegate metadata without injecting interface fields."""
    metadata = {"command": "status"}

    with patch("zenml.analytics.track", return_value=True) as track_mock:
        result = track(AnalyticsEvent.CLI_INVOKED, metadata)

    assert result is True
    track_mock.assert_called_once_with(
        event=AnalyticsEvent.CLI_INVOKED,
        metadata=metadata,
    )
